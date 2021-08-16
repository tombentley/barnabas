/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.util.Set;

/**
 * <p>Manages the certificate issuing entity (i.e. CO or UO, depending on PKI) part of the shared-state protocol for managing trust.
 * See {@link TrustController} for the full state machine.
 */
public interface TrustDependencyManager {

    /**
     * Request a root cert be trusted.
     * Updates the state for that cert to {@link TrustState#UNTRUSTED}.
     * @param rootCert The cert
     */
    void requireTrust(X509Certificate rootCert);

    /**
     * Records that a root cert is no being used.
     * Updates the state for that cert to {@link TrustState#TRUSTED_USED}.
     * @param rootCert The cert
     */
    void depend(X509Certificate rootCert);

    /**
     * Records that a root cert is no longer being used.
     * Updates the state for that cert to {@link TrustState#ZOMBIE}.
     * @param rootCert The cert
     */
    // TODO arrange for the CO and UO to call this
    void undepend(X509Certificate rootCert);

    /**
     * Gets the state of the certificate with the given {@code rootCaFingerprint}.
     * @param rootCaFingerprint The fingerprint
     * @return The state of the certificate with this fingerprint, or null if not such certificate exists.
     */
    TrustState stateOfRootCert(Fingerprint rootCaFingerprint);

    /**
     * Gets all the certificates in any of the given {@code states}.
     * @param states The states.
     * @return The certificates in those states.
     */
    Set<X509Certificate> rootCertsInStates(Set<TrustState> states);
}
