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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Replaceable;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

// TODO Possibly there's a 3rd interface for the Chain/CA bootstrap part
/**
 * <p>Manages a set of trust roots and the state of their trust relationships.
 * This is done using a Secret shared between an "issuing entity" and a "trust managing entity".
 * A shared Secret is used because the issuing entity and the trust managing entity are not necessarily in the
 * same process.</p>
 *
 * <p>For the client's PKI the issuing entity is the {@link CertificateController} in the UO and the trust managing entity
 * is the CO.
 * For the cluster PKI the issuing entity is the {@link CertificateController} in the CO and the trust managing entity
 * is the KAO in CO (which knows when pods have been rolled) following changes in trust stores.</p>.
 *
 * <p>Each trust root is represented by a (root) CA certificate and is accompanied by an
 *    {@linkplain TrustState explicit state} with well-defined transitions:</p>
 * <dl>
 *     <dt>new -&gt; {@link TrustState#UNTRUSTED}</dt>
 *     <dd>(note: new is an in-memory state only).
 *     On observing a new root CA cert,  the issuing entity
 *     calls {@link #requireTrust(X509Certificate)} to request that it become trusted.</dd>
 *
 *     <dt>UNTRUSTED -&gt; {@link TrustState#TRUSTED_NEW}</dt>
 *     <dd>On observing a certificate in the UNTRUSTED state, the trust managing entity performs the orchestration necessary for the
 *     certificate to become trusted. Once this is complete it transitions the trust root to TRUSTED_NEW by calling
 *     {@link #trustRootCerts(Set)}</dd>
 *
 *     <dt>TRUSTED_NEW -&gt; {@link TrustState#TRUSTED_USED}</dt>
 *     <dd>On observing a certificate in the TRUSTED_NEW state, the issuing entity can deploy the
 *     end entity certificate and transition the trust root to TRUSTED_USED using {@link #depend(X509Certificate)}.
 *     This indicates that the trust root is now depended on by at least one EE certificate, and therefore cannot
 *     be simply deleted.</dd>
 *
 *     <dt>{@link TrustState#TRUSTED_USED} -&gt; {@link TrustState#ZOMBIE}</dt>
 *     <dd>At some later time the issuing entity will find that none of the required certificates
 *     depend on the trust root any more. When this happens it calls {@link #undepend(X509Certificate)} to
 *     trigger transition to the ZOMBIE state.</dd>
 *
 *     <dt>Deletion</dt>
 *     <dd>On observing a certificate in the ZOMBIE state the trust managing entity arranges to remove it from the relevant
 *     trust stores, and then calls {@link #delete(Set)} to remove the certificate from the shared Secret.</dd>
 * </dl>
 * <p>Although this is described as a state machine, this class does not currently enforce that these are the only
 * allowed transitions.</p>
 *
 * <p>The shared Secret looks something like this</p>
 * <pre><code>
 * kind: Secret
 *   metadata:
 *   # the input secret
 *   name: $CLUSTER-$PKI-trust # e.g. my-cluster-clients-trust
 * data:
 *   $FINGERPRINT.crt: ... # The certificate
 *   $FINGERPRINT.state: ... # The certificate's state
 * </code></pre>
 * <p>Note: The secret can contain multiple certificates.</p>
 */
public class TrustController implements TrustManager, TrustDependencyManager {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(TrustController.class);
    public static final String CERT_SUFFIX = ".crt";
    public static final String STATE_SUFFIX = ".state";

    private final NamedResource<Secret> secretResource;
    private final Reconciliation reconciliation;
    private final OwnerReference ownerReference;

    public TrustController(Reconciliation reconciliation, NamedResource<Secret> secretResource,
                           OwnerReference ownerReference) {
        this.reconciliation = reconciliation;
        this.secretResource = secretResource;
        this.ownerReference = ownerReference;
    }

    public static String encode(TrustState state) {
        return Base64.getEncoder().encodeToString(state.id().getBytes(StandardCharsets.US_ASCII));
    }
    public static TrustState decodeState(String str) {
        return TrustState.fromId(new String(Base64.getDecoder().decode(str), StandardCharsets.US_ASCII));
    }

    public static String encode(X509Certificate cert) throws CertificateEncodingException {
        return Base64.getEncoder().encodeToString(PkiUtils.asPemString(cert).getBytes(StandardCharsets.US_ASCII));
    }

    public static X509Certificate decodeCert(String str) throws CertificateException {
        return PkiUtils.x509Certificate(Base64.getDecoder().decode(str));
    }

    /**
     * Replaces the secret using a replacement computed using the given {@code fn}, using a random
     * exponential backoff in the event of conflict. As such {@code fn} can be called more than once.
     * @param fn The operation to apply.
     */
    private void replace(UnaryOperator<Secret> fn) {
        int delayBoundMs = 100;
        while (true) {
            Secret oldSecret = secretResource.get();
            if (oldSecret == null) {
                oldSecret = secretResource.create(new SecretBuilder().withNewMetadata().withOwnerReferences(ownerReference).endMetadata().build());
            }
            Replaceable<Secret> secretReplaceable = secretResource.lockResourceVersion(oldSecret.getMetadata().getResourceVersion());
            if (oldSecret.getData() == null) {
                oldSecret.setData(Map.of());
            }
            Secret replacement = fn.apply(oldSecret);
            if (LOGGER.isDebugEnabled()) {
                logTransitions(oldSecret.getData(), replacement.getData());
            }
            if (!oldSecret.getData().equals(replacement.getData())) {
                try {
                    secretReplaceable.replace(replacement);
                    return;
                } catch (KubernetesClientException e) {
                    if (e.getCode() == 409 && delayBoundMs < 10_000) {
                        try {
                            int delayMs = new Random().nextInt(delayBoundMs);
                            LOGGER.debugCr(reconciliation, "Conflict updating {} retrying in {}ms", secretResource, delayMs);
                            Thread.sleep(delayMs);
                        } catch (InterruptedException interruptedException) {
                            throw new RuntimeException(interruptedException);
                        }
                        delayBoundMs *= 2;
                    } else {
                        throw e;
                    }
                }
            } else {
                LOGGER.debugCr(reconciliation, "Replacement of {} would be a noop", secretResource);
                return;
            }
        }
    }

    private void logTransitions(Map<String, String> oldData, Map<String, String> replacementData) {
        oldData.entrySet().stream()
                .filter(oldEntry -> isStateItem(oldEntry.getKey()))
                .forEach(oldEntry -> {
                    String oldKey = oldEntry.getKey();
                    String oldValue = oldEntry.getValue();
                    String replacementValue = replacementData.get(oldKey);
                    if (replacementValue == null) {
                        LOGGER.infoCr(reconciliation, "Cert {} in {}: Removed with state '{}'",
                                fingerprint(oldKey), secretResource, decodeState(oldValue));
                    } else if (replacementValue.equals(oldValue)) {
                        LOGGER.debugCr(reconciliation, "Cert {} in {}: Unchanged '{}'",
                                fingerprint(oldKey), secretResource, decodeState(oldValue));
                    } else {
                        LOGGER.infoCr(reconciliation, "Cert {} in {}: Transition '{}' -> '{}'",
                                fingerprint(oldKey), secretResource,
                                decodeState(oldValue), decodeState(replacementValue));
                    }
                });
        replacementData.entrySet().stream()
                .filter(replacementEntry -> isStateItem(replacementEntry.getKey()))
                .forEach(replacementEntry -> {
                    String replacementKey = replacementEntry.getKey();
                    String replacementValue = replacementEntry.getValue();
                    String oldValue = oldData.get(replacementKey);
                    if (oldValue == null) {
                        LOGGER.infoCr(reconciliation, "Cert {} in {}: Added in state '{}'",
                                fingerprint(replacementKey), secretResource, decodeState(replacementValue));
                    }
                });
    }

    // Called by UO to request a root cert be trusted
    // i.e. add t S with state UNTRUSTED
    // xxx.cert=...
    // xxx.state=untrusted
    @Override
    public void requireTrust(X509Certificate rootCert) {
        replace(oldSecret -> {
            try {
                TrustState newState = TrustState.UNTRUSTED;
                Fingerprint fingerprint = PkiUtils.fingerprint(rootCert);
                String stateItem = stateKey(fingerprint);
                return new SecretBuilder(oldSecret)
                        .addToData(stateItem, encode(newState))
                        .addToData(certKey(fingerprint), encode(rootCert))
                        .build();
            } catch (CertificateEncodingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    // Called by the CO to find the roots which need to be trusted (i
    // i.e. all those in C with state UNTRUSTED
    @Override
    public Set<X509Certificate> requiringTrust() {
        return rootCertsInStates(Set.of(TrustState.UNTRUSTED));
    }

    // TODO all these methods for transitioning states are very similar
    // Refactor!!

    // Called by CO to say that those certs are now trusted
    // i.e. update S with state TRUSTED
    // xxx.cert=...
    // xxx.state=trusted_new
    @Override
    public void trustRootCerts(Set<X509Certificate> rootCerts) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            for (X509Certificate cert : rootCerts) {
                Fingerprint fingerprint = PkiUtils.fingerprint(cert);
                sb.addToData(stateKey(fingerprint), encode(TrustState.TRUSTED_NEW));
            }
            return sb.build();
        });
    }

    @Override
    public void phaseOut(Set<X509Certificate> rootCerts) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            for (X509Certificate cert : rootCerts) {
                Fingerprint fingerprint = PkiUtils.fingerprint(cert);
                sb.addToData(stateKey(fingerprint), encode(TrustState.PHASE_OUT));
            }
            return sb.build();
        });
    }

    // Called by the UO when an EE certificate is "deployed" and the root is now depended on
    // ie. update S with a count
    // xxx.cert=...
    // xxx.state=trusted_used
    @Override
    public void depend(X509Certificate rootCert) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            Fingerprint fingerprint = PkiUtils.fingerprint(rootCert);
            sb.addToData(stateKey(fingerprint), encode(TrustState.TRUSTED_USED));
            return sb.build();
        });
    }

    // Called by the UO when the root certificate is no longer depended on by any EE certs
    // ie. update S with a count
    // xxx.cert=...
    // xxx.state=zombie
    @Override
    public void undepend(X509Certificate rootCert) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            Fingerprint fingerprint = PkiUtils.fingerprint(rootCert);
            sb.addToData(stateKey(fingerprint), encode(TrustState.ZOMBIE));

            return sb.build();
        });
    }

    @Override
    public TrustState stateOfRootCert(Fingerprint rootCaFingerprint) {
        TrustState result = null;
        Secret secret = secretResource.get();
        if (secret != null
                && secret.getData() != null) {
            String s = secret.getData().get(stateKey(rootCaFingerprint));
            if (s != null) {
                result = decodeState(s);
            }
        }
        return result;
    }

    @Override
    public Set<X509Certificate> rootCertsInStates(Set<TrustState> states) {
        Set<X509Certificate> result;
        Secret secret = secretResource.get();
        if (secret == null
                || secret.getData() == null
                || secret.getData().isEmpty()) {
            result = Set.of();
        } else {
            var entries = secret.getData().entrySet();
            Set<String> collect = entries.stream()
                    .filter(e -> isStateItem(e.getKey()) && states.contains(decodeState(e.getValue())))
                    .map(e -> fingerprint(e.getKey()))
                    .collect(Collectors.toSet());
            result = entries.stream()
                    .filter(entry -> isCertItem(entry.getKey())
                            && collect.contains(fingerprint(entry.getKey())))
                    .map(entry -> {
                        try {
                            return decodeCert(entry.getValue());
                        } catch (CertificateException e) {
                            throw new RuntimeException("Could not decode certificate in item " + entry.getValue() + " of " + secretResource, e);
                        }
                    })
                    .collect(Collectors.toSet());

        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debugCr(reconciliation, "Certs in {} in states {}: {}", secretResource, states,
                    result.stream()
                            .map(PkiUtils::fingerprint)
                            .map(Object::toString)
                            .collect(Collectors.joining(", ", "[", "]")));
        }
        return result;
    }

    public static String fingerprint(String key) {
        if (isStateItem(key)) {
            return key.substring(0, key.length() - STATE_SUFFIX.length());
        } else if (isCertItem(key)) {
            return key.substring(0, key.length() - CERT_SUFFIX.length());
        } else {
            throw new IllegalStateException("Unexpected item in Secret: " + key);
        }
    }

    public static boolean isStateItem(String key) {
        return key.endsWith(STATE_SUFFIX);
    }

    public static boolean isCertItem(String key) {
        return key.endsWith(CERT_SUFFIX);
    }

    public static String stateKey(Fingerprint fingerprint) {
        return fingerprint + STATE_SUFFIX;
    }

    public static String certKey(Fingerprint fingerprint) {
        return certKey(fingerprint.toString());
    }

    public static String certKey(String fingerprint) {
        return fingerprint + CERT_SUFFIX;
    }

    /**
     * Find the roots which need to be trusted.
     * @return The certs with state ZOMBIE.
     */
    // Called by the CO to find the root which are no longer needed
    @Override
    public Set<X509Certificate> zombies() {
        return rootCertsInStates(Set.of(TrustState.ZOMBIE));
    }

    /**
     * Deletes certs in the ZOMBIE state.
     * @param certs The certs
     */
    // Called by the CO to delete a set of roots which are in the zombie state
    @Override
    public void delete(Set<X509Certificate> certs) {
        Set<String> collect = certs.stream()
                .flatMap(cert -> {
                    Fingerprint fingerprint = PkiUtils.fingerprint(cert);
                    LOGGER.debugCr(reconciliation, "Deleting {} from {}", fingerprint, secretResource);
                    return Stream.of(
                            stateKey(fingerprint),
                            certKey(fingerprint));
                })
                .collect(Collectors.toSet());
        replace(oldSecret -> {
            var secret = new SecretBuilder(oldSecret).build();
            secret.getData().keySet().removeAll(collect);
            return secret;
        });
    }
}
