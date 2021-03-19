/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Implements the Trust State Machine by consuming a set of trust roots with their desired states
 * and reconciling that with their actual states.</p>
 *
 * <p>Where the desired states come from depends on the PKI concerned.
 * For the cluster CA they're supplied by the CO (based on the certificates issued by the CertificateController).
 * For the clients CA we need some shared state, such as a Secret:</p>
 * <pre><code>
 * kind: Secret
 *   metadata:
 *   # the input secret
 *   name:	my-cluster-clients-ca-input
 * data:
 *   fingerprint.state: ... # what the current state should be (what happens if this is an illegal state transition??)
 * </code></pre>
 * <p>But that's done outside this class.</p>
 */
public class TrustController {

    private static final Logger LOGGER = LogManager.getLogger(TrustController.class);

    /**
     * <pre><code>
     * kind: Secret
     * metadata:
     *   # the private state
     *   name:	$CLUSTER-$CA
     * data:
     *   ca.key: ... # legacy (for downgrade, will eventually be removed)
     *   $FINGERPRINT.p12: ... # contains key and cert
     *   $FINGERPRINT.state: ... # is the current state
     * </code></pre>
     */
    static class PrivateState {

        private static final Pattern PATTERN;
        static {
            // TODO should we use CaCertState::name or decouple the identifier name from the name of the state as stored?
            PATTERN = Pattern.compile("(?<fingerprint>[0-9A-Z]{" + Fingerprint.HEX_LENGTH + "})\\.(?<type>state|crt)");
        }
        private final Resource<Secret> secretResource;

        public PrivateState(Resource<Secret> secretResource) {
            this.secretResource = secretResource;
        }

        Map<Fingerprint, TrustRoot> load() {
            Secret secret = secretResource.get();
            if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
                return Map.of();
            } else {
                Map<Fingerprint, TrustRoot> result = new HashMap<>();
                Map<String, String> data = secret.getData();
                for (var entry : data.entrySet()) {
                    Matcher matcher = PATTERN.matcher(entry.getKey());
                    if (matcher.matches()) {
                        String type = matcher.group("type");
                        Fingerprint claimedFingerprint = new Fingerprint(matcher.group("fingerprint"));
                        if (!result.containsKey(claimedFingerprint)) {
                            TrustState state;
                            String crt;
                            if ("state".equals(type)) {
                                state = TrustState.valueOf(entry.getValue());
                                crt = data.get(claimedFingerprint + "." + state.name() + ".crt");
                            } else {
                                String stateStr = data.get(claimedFingerprint + ".state");
                                state = stateStr != null ? TrustState.valueOf(stateStr) : null;
                                crt = entry.getValue();
                            }
                            if (crt != null && state != null) {
                                try {
                                    X509Certificate x509Certificate = PkiUtils.x509Certificate(crt.getBytes(StandardCharsets.ISO_8859_1));
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
            Map<String, String> data = new HashMap<>();
            for (var entry : trustRoots.entrySet()) {
                Fingerprint fingerprint = entry.getKey();
                if (fingerprint.equals(entry.getValue().fingerprint())) {
                    try {
                        data.put(fingerprint + ".crt", PkiUtils.asPemString(entry.getValue().x509()));
                        data.put(fingerprint + ".state", entry.getValue().state().name());
                    } catch (CertificateEncodingException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new RuntimeException();
                }
            }
            Secret secret = secretResource.get();
            secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret).withData(data)).build());
        }
    }

    private PrivateState privateState;
    private List<TrustOutputWriter> outputWriters;

    public TrustController(Resource<Secret> secretResource, List<TrustOutputWriter> outputWriters) {
        this.privateState = new PrivateState(secretResource);
        this.outputWriters = outputWriters;
    }

    /**
     * Reconcile the current state with the desired state
     * @param desired The desired state
     * @return true if there were any differences
     * @throws CertificateEncodingException
     */
    public boolean reconcile(List<TrustRoot> desired) throws CertificateEncodingException {
        // Load from private state.
        Map<Fingerprint, TrustRoot> current = privateState.load();

        // reconcile the trust input with the private state making transitions in-memory.
        Map<Fingerprint, TrustRoot> desiredMap = desired.stream().collect(Collectors.toMap(TrustRoot::fingerprint, Function.identity()));
        Map<Fingerprint, TrustRoot> result = new HashMap<>(desiredMap.size());
        for (var entry : current.entrySet()) {
            var f = entry.getKey();
            TrustRoot currentRoot = entry.getValue();
            final TrustRoot newRoot;
            if (desiredMap.containsKey(f)) {
                // Do their states differ?
                TrustRoot desiredRoot = desiredMap.get(f);
                if (currentRoot.state() != desiredRoot.state()) {
                    newRoot = transition(currentRoot, desiredRoot);
                } else {
                    newRoot = currentRoot;
                }
                result.put(newRoot.fingerprint(), newRoot);
            } else {
                // Deleted
                newRoot = deleted(currentRoot);
            }
            if (newRoot != null) {
                result.put(newRoot.fingerprint(), newRoot);
            }
        }
        for (var entry : desiredMap.entrySet()) {
            if (!current.containsKey(entry.getKey())) {
                // New
                TrustRoot newRoot = added(entry.getValue());
                result.put(newRoot.fingerprint(), newRoot);
            }
        }

        // Persist the private state
        privateState.store(result);
        // Update the trust outputs
        for (TrustOutputWriter writer : outputWriters) {
            writer.write(result.values());
        }
        return result.equals(current);
    }

    private TrustRoot added(TrustRoot value) {
        return null;
    }

    private TrustRoot deleted(TrustRoot currentRoot) {
        return null;
    }

    private TrustRoot transition(TrustRoot currentRoot, TrustRoot desiredRoot) {
        return null;
    }
}
