/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.util.Set;

/**
 * <p>Manages the trust managing entity (i.e. CO) part of the shared-state protocol for managing trust.
 * See {@link TrustController} for the full state machine.
 */
public interface TrustManager {
    /**
     * Find the roots which need to be trusted.
     * @return The certs with state {@link TrustState#UNTRUSTED}.
     */
    Set<X509Certificate> requiringTrust();

    /**
     * Records that a set of root certs are now trusted.
     * Updates the states for each cert to {@link TrustState#TRUSTED_UNUSED}.
     * @param rootCerts The certs
     */
    // TODO Does this need to be batched
    void trustRootCerts(Set<X509Certificate> rootCerts);

    void phaseOut(Set<X509Certificate> rootCerts);

    /**
     * Find the roots which need to be trusted.
     * @return The certs with state {@link TrustState#ZOMBIE}.
     */
    Set<X509Certificate> zombies();

    // TODO Does this need to be batched
    void delete(Set<X509Certificate> certs);
}
