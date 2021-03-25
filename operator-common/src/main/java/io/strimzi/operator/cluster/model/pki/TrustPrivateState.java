/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.nio.charset.StandardCharsets;
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

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Stores the a root CA certificate and the trust state of that certificate.</p>
 * This state is considered "private" and is stored separately from any of
 * the {@link TrustOutputWriter} (which are intended to be consumed by client applications, thus "public").
 * <pre><code>
 * kind: Secret
 * metadata:
 *   # the private state
 *   name:	$CLUSTER-$CA
 * data:
 *   $FINGERPRINT.crt: ... # contains key and cert
 * stringData:
 *   $FINGERPRINT.state: ... # is the current state
 * </code></pre>
 */
class TrustPrivateState {

    private static final Pattern PATTERN;
    static {
        PATTERN = Pattern.compile("(?<fingerprint>[0-9A-Z]{" + Fingerprint.HEX_LENGTH + "})\\.crt");
    }
    private static final Logger LOGGER = LogManager.getLogger(TrustPrivateState.class);

    private final Resource<Secret> secretResource;

    public TrustPrivateState(Resource<Secret> secretResource) {
        this.secretResource = Objects.requireNonNull(secretResource);
    }

    Map<Fingerprint, TrustRoot> load() {
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
                                    result.put(claimedFingerprint, new TrustRoot(x509Certificate, state));
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

    void store(Map<Fingerprint, TrustRoot> trustRoots) {
        Map<String, String> data = new HashMap<>(trustRoots.size());
        Map<String, String> stringData = new HashMap<>(trustRoots.size());
        for (var entry : trustRoots.entrySet()) {
            Fingerprint fingerprint = entry.getKey();
            if (fingerprint.equals(entry.getValue().fingerprint())) {
                try {
                    data.put(fingerprint + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(entry.getValue().x509())));
                    stringData.put(fingerprint + ".state", entry.getValue().state().id());
                } catch (CertificateEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException();
            }
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret))
                .withData(data)
                .withStringData(stringData)
                .build());
    }
}
