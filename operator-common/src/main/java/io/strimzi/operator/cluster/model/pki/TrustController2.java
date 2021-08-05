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
 *     {@link #haveTrust(Set)}</dd>
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
public class TrustController2 implements TrustManager, UseManager {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(TrustController2.class);
    public static final String CERT_SUFFIX = ".crt";
    public static final String STATE_SUFFIX = ".state";

    private final NamedResource<Secret> secretResource;
    private final Reconciliation reconciliation;

    public TrustController2(Reconciliation reconciliation, NamedResource<Secret> secretResource) {
        this.reconciliation = reconciliation;
        this.secretResource = secretResource;
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
                oldSecret = secretResource.create(new Secret());
            }
            Replaceable<Secret> secretReplaceable = secretResource.lockResourceVersion(oldSecret.getMetadata().getResourceVersion());
            if (oldSecret.getData() == null) {
                oldSecret.setData(Map.of());
            }
            Secret build = fn.apply(oldSecret);
            try {
                secretReplaceable.replace(build);
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
        }
    }

    /**
     * Request a root cert be trusted.
     * Updates the state for that cert to UNTRUSTED
     * @param cert The cert
     */
    // Called by UO to request a root cert be trusted
    // i.e. add t S with state UNTRUSTED
    // xxx.cert=...
    // xxx.state=untrusted
    @Override
    public void requireTrust(X509Certificate cert) {
        replace(oldSecret -> {
            try {
                SecretBuilder sb = new SecretBuilder(oldSecret);

                Fingerprint fingerprint = PkiUtils.fingerprint(cert);
                LOGGER.debugCr(reconciliation, "Transitioning {} to {} in {}", fingerprint, TrustState.UNTRUSTED, secretResource);
                String certItem = certKey(fingerprint);
                if (oldSecret.getData().containsKey(certItem)) {
                    throw new RuntimeException(secretResource + " already contains item " + certItem);
                }
                sb.addToData(certItem, encode(cert));
                String stateItem = stateKey(fingerprint);
                if (oldSecret.getData().containsKey(stateItem)) {
                    throw new RuntimeException(secretResource + " already contains item " + stateItem);
                }
                sb.addToData(stateItem, encode(TrustState.UNTRUSTED));

                return sb.build();
            } catch (CertificateEncodingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Find the roots which need to be trusted.
     * @return The certs with state UNTRUSTED
     */
    // Called by the CO to find the roots which need to be trusted (i
    // i.e. all those in C with state UNTRUSTED
    @Override
    public Set<X509Certificate> requiringTrust() {
        return certsInStates(Set.of(TrustState.UNTRUSTED));
    }

    /**
     * Records that a set of root certs are now trusted.
     * Updates the states for each cert to TRUSTED_NEW
     * @param certs The certs
     */
    // Called by CO to say that those certs are now trusted
    // i.e. update S with state TRUSTED
    // xxx.cert=...
    // xxx.state=trusted_new
    @Override
    public void haveTrust(Set<X509Certificate> certs) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            for (X509Certificate cert : certs) {
                Fingerprint fingerprint = PkiUtils.fingerprint(cert);
                LOGGER.debugCr(reconciliation, "Transitioning {} to {} in {}", fingerprint, TrustState.TRUSTED_NEW, secretResource);
                sb.addToData(stateKey(fingerprint), encode(TrustState.TRUSTED_NEW));
            }
            return sb.build();
        });
    }

    /**
     * Records that a root cert is no being used.
     * Updates the state for that cert to TRUSTED_USED
     * @param cert The cert
     */
    // Called by the UO when an EE certificate is "deployed" and the root is now depended on
    // ie. update S with a count
    // xxx.cert=...
    // xxx.state=trusted_used
    @Override
    public void depend(X509Certificate cert) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            Fingerprint fingerprint = PkiUtils.fingerprint(cert);
            LOGGER.debugCr(reconciliation, "Transitioning {} to {} in {}", fingerprint, TrustState.TRUSTED_USED, secretResource);
            sb.addToData(stateKey(fingerprint), encode(TrustState.TRUSTED_USED));
            return sb.build();
        });
    }

    /**
     * Records that a root cert is no longer being used.
     * Updates the state for that cert to ZOMBIE
     * @param cert The cert
     */
    // Called by the UO when the root certificate is no longer depended on by any EE certs
    // ie. update S with a count
    // xxx.cert=...
    // xxx.state=zombie
    @Override
    public void undepend(X509Certificate cert) {
        replace(oldSecret -> {
            SecretBuilder sb = new SecretBuilder(oldSecret);
            Fingerprint fingerprint = PkiUtils.fingerprint(cert);
            LOGGER.debugCr(reconciliation, "Transitioning {} to {} in {}", fingerprint, TrustState.ZOMBIE, secretResource);
            sb.addToData(stateKey(fingerprint), encode(TrustState.ZOMBIE));

            return sb.build();
        });
    }

    @Override
    public TrustState state(Fingerprint rootCaFingerprint) {
        Secret secret = secretResource.get();
        if (secret == null
                || secret.getData() == null) {
            return null;
        } else {
            String s = secret.getData().get(stateKey(rootCaFingerprint));
            if (s == null) {
                return null;
            } else {
                return decodeState(s);
            }
        }
    }

    @Override
    public Set<X509Certificate> certsInStates(Set<TrustState> states) {
        Secret secret = secretResource.get();
        if (secret == null
                || secret.getData() == null
                || secret.getData().isEmpty()) {
            return Set.of();
        } else {
            var entries = secret.getData().entrySet();
            Set<String> collect = entries.stream()
                    .filter(e -> isStateItem(e.getKey()) && states.contains(decodeState(e.getValue())))
                    .map(e -> fingerprint(e.getKey()))
                    .collect(Collectors.toSet());
            return entries.stream()
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
    }

    public static String fingerprint(String key) {
        if (isStateItem(key)) {
            return key.substring(0, key.length() - STATE_SUFFIX.length());
        } else if (isCertItem(key)) {
            return key.substring(0, key.length() - CERT_SUFFIX.length());
        } else {
            throw new IllegalStateException();
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
        return certsInStates(Set.of(TrustState.ZOMBIE));
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
