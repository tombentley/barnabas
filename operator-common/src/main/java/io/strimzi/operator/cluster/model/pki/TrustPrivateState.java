/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Replaceable;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Stores the a root CA certificate and the trust state of that certificate.
 * This state is considered "private" and is stored separately from any of
 * the {@link TrustOutputWriter} (which are intended to be consumed by client applications, thus "public").
 * The format of the Secret written and read looks like this:</p>
 * <pre><code>
 * kind: Secret
 * metadata:
 *   # the private state
 *   name: $CLUSTER-$CA
 * data:
 *   $FINGERPRINT.crt: ... # contains key and cert
 * stringData:
 *   $FINGERPRINT.state: ... # is the current state
 * </code></pre>
 * <p>where there may be multiple fingerprints present.</p>
 *
 * <p>The Secret state is read and written by the {@link CertificateController} and the {@link TrustController}
 * and they may be operating in separate processes (e.g. for the client's PKI, the {@code CertificateController} is
 * in the UO and the {@code TrustController} is in the CO. This is safe because they are each concerned with
 * different transitions of the {@link TrustState} state machine, as follows:</p>
 * <dl>
 *     <dt>{@code -> UNTRUSTED}</dt>
 *     <dd>The {@code CertificateController} adds a CA certificate in the UNTRUSTED state</dd>
 *
 *     <dt>{@code UNTRUSTED -> TRUSTED}</dt>
 *     <dd>The {@code TrustController} does the transition to TRUSTED</dd>
 *
 *     <dt>{@code TRUSTED -> PHASE_OUT}</dt>
 *     <dd>???</dd>
 *
 *     <dt>{@code PHASE_OUT -> ZOMBIE}</dt>
 *     <dd>???</dd>
 * </dl>
 * <p>The changes to Kube could be protected from concurrent modification in one of two ways</p>
 * <ul>
 *     <li>Using a replace, conditional upon the resourceVersion, using {@link Resource#lockResourceVersion(String)}.
 *     This supports only course concurrency and so would scale poorly in the presence of a large number of
 *     certificates.</li>
 *     <li>Because each transition is only done by one party it would be possible to use an
 *     <a href="https://tools.ietf.org/html/rfc6902#section-4.6">RFC-6902 JSON patch</a> (in more concrete HTTP terms,
 *     a {@code PATCH} request with {@code Content-Type: application/json-patch+json})
 *     with {@code "op"="test"} guarding a {@code "op"="replace"}. This would support finer-grained concurrent
 *     changes.</li>
 * </ul>
 * <p>We currently implement the former, since we expect a small number of trusted certs and we have the APIs
 * to implement it easily.</p>
 */
public class TrustPrivateState {

    private static final Pattern PATTERN;
    static {
        PATTERN = Pattern.compile("(?<fingerprint>[0-9A-Z]{" + Fingerprint.HEX_LENGTH + "})\\.crt");
    }
    private static final Logger LOGGER = LogManager.getLogger(TrustPrivateState.class);

    private final Resource<Secret> secretResource;

    public TrustPrivateState(Resource<Secret> secretResource) {
        this.secretResource = Objects.requireNonNull(secretResource);
    }
    /*
     * API should probably be existing LOAD
     * plus TRANSITION (for changing the state of an existing certificate)
     * plus REMOVE (for deleting a certificate, though maybe this too is a transition)
     * We also need each TrustRoot to remember the resourceVersion is was loaded from.
     */

    public Map<Fingerprint, TrustRoot> load() {
        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return Map.of();
        } else {
            Map<Fingerprint, TrustRoot> result = new HashMap<>();
            Map<String, String> data = secret.getData();
            Map<String, String> stringData = secret.getStringData();
            for (var entry : data.entrySet()) {
                Matcher matcher = PATTERN.matcher(entry.getKey());
                if (matcher.matches()) {
                    Fingerprint claimedFingerprint = new Fingerprint(matcher.group("fingerprint"));
                    if (!result.containsKey(claimedFingerprint)) {
                        String stateStr = stringData.get(claimedFingerprint + ".state");
                        TrustState state = stateStr != null ? TrustState.fromId(stateStr) : null;
                        String crt = entry.getValue();
                        if (crt != null && state != null) {
                            try {
                                X509Certificate x509Certificate = PkiUtils.x509Certificate(Base64.getDecoder().decode(crt));
                                Fingerprint actualFingerprint = PkiUtils.fingerprint(x509Certificate);
                                if (actualFingerprint.equals(claimedFingerprint)) {
                                    result.put(claimedFingerprint, TrustRoot.load(x509Certificate, state, secret.getMetadata().getResourceVersion()));
                                } else {
                                    LOGGER.warn("Secret entry for trust root claims fingerprint different from certificate's actual fingerprint:" +
                                                    "claimed: {}, actual: {}, entry: {}, secret: {}, namespace: {}",
                                            claimedFingerprint, actualFingerprint,
                                            entry.getKey(),
                                            secret.getMetadata().getName(), secret.getMetadata().getNamespace());
                                }
                            } catch (CertificateException e) {
                                LOGGER.warn("Secret contains certificate entry which can't be parsed:" +
                                                "entry: {}, secret: {}, namespace: {}",
                                        entry.getKey(),
                                        secret.getMetadata().getName(), secret.getMetadata().getNamespace(), e);
                            }
                        } else if (crt != null) {
                            LOGGER.warn("Secret contains entry for a certificate, but no matching entry for its state:" +
                                            "entry: {}, secret: {}, namespace: {}",
                                    entry.getKey(),
                                    secret.getMetadata().getName(), secret.getMetadata().getNamespace());
                        } else if (state != null) {
                            LOGGER.warn("Secret contains entry for a state, but no matching entry for its certificate:" +
                                            "entry: {}, secret: {}, namespace: {}",
                                    entry.getKey(),
                                    secret.getMetadata().getName(), secret.getMetadata().getNamespace());
                        }
                    }
                } else {
                    LOGGER.warn("Secret contains unexpected entry:" +
                                    "entry: {}, secret: {}, namespace: {}",
                            entry.getKey(),
                            secret.getMetadata().getName(), secret.getMetadata().getNamespace());
                }
            }
            return Collections.unmodifiableMap(result);
        }
    }

    /**
     * Transitions persists the given roots in their current states.
     * This is not a "set" operation: Any stored roots which were present before this call
     * will still be present in the store afterwards. To remove roots use {@link #remove(Map)}
     * @param trustRoots The trust roots.
     */
    public void transition(Map<Fingerprint, TrustRoot> trustRoots) {
        if (trustRoots.isEmpty()) {
            return;
        }
        Map<String, String> data = new HashMap<>(trustRoots.size());
        Map<String, String> stringData = new HashMap<>(trustRoots.size());
        String resourceVersion = null;
        for (var entry : trustRoots.entrySet()) {
            Fingerprint fingerprint = entry.getKey();
            TrustRoot trustRoot = entry.getValue();
            if (resourceVersion == null) {
                resourceVersion = trustRoot.resourceVersion();
            } else if (trustRoot.state() != TrustState.UNTRUSTED
                    && !resourceVersion.equals(trustRoot.resourceVersion())) {
                throw new IllegalArgumentException("Given roots loaded from multiple versions of the secret");
            }
            if (fingerprint.equals(trustRoot.fingerprint())) {
                try {
                    data.put(fingerprint + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(trustRoot.x509())));
                    stringData.put(fingerprint + ".state", trustRoot.state().id());
                } catch (CertificateEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException();
            }
        }
        Secret secret = secretResource.get();
        if (secret == null) {
            secretResource.create(new SecretBuilder().withData(data)
                        .withStringData(stringData)
                    .build());
        } else {
            if (resourceVersion == null) {
                resourceVersion = secret.getMetadata().getResourceVersion();
            }
            Replaceable<Secret> secretReplaceable = secretResource.lockResourceVersion(resourceVersion);
            SecretBuilder secretBuilder = new SecretBuilder(secret)
                    .editMetadata()
                    .withResourceVersion(resourceVersion)
                    .endMetadata();
            for (var e : data.entrySet()) {
                secretBuilder.addToData(e.getKey(), e.getValue());
            }
            for (var e : stringData.entrySet()) {
                secretBuilder.addToStringData(e.getKey(), e.getValue());
            }
            secretReplaceable.replace(
                    secretBuilder.build());
        }
    }

    public void remove(Map<Fingerprint, TrustRoot> trustRoots) {
        if (trustRoots.isEmpty()) {
            return;
        }
        Secret secret = secretResource.get();
        if (secret != null) {
            String resourceVersion = null;
            for (var entry : trustRoots.entrySet()) {
                Fingerprint fingerprint = entry.getKey();
                TrustRoot trustRoot = entry.getValue();
                if (resourceVersion == null) {
                    resourceVersion = trustRoot.resourceVersion();
                } else if (!resourceVersion.equals(trustRoot.resourceVersion())) {
                    throw new IllegalArgumentException("Given roots loaded from multiple versions of the secret");
                }
            }
            Map<String, String> data = secret.getData();
            Map<String, String> stringData = secret.getStringData();
            data.keySet().removeAll(trustRoots.keySet().stream().map(fingerprint1 -> fingerprint1.toString() + ".crt").collect(Collectors.toSet()));
            stringData.keySet().removeAll(trustRoots.keySet().stream().map(fingerprint1 -> fingerprint1.toString() + ".state").collect(Collectors.toSet()));
            secretResource.lockResourceVersion(resourceVersion).replace(
                    new SecretBuilder(secret)
                            .editMetadata()
                                .withResourceVersion(resourceVersion)
                            .endMetadata()
                            .withData(data)
                            .withStringData(stringData)
                            .build());
        }
    }
}
