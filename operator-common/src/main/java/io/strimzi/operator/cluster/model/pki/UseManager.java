/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.util.Set;

/**
 * <p>Manages the certificate issuing entity (i.e. CO or UO, depending on PKI) part of the shared-state protocol for managing trust.
 * See {@link TrustController2} for the full state machine.
 */
public interface UseManager {
    void requireTrust(X509Certificate cert);

    void depend(X509Certificate cert);

    void undepend(X509Certificate cert);

    TrustState state(Fingerprint rootCaFingerprint);

    Set<X509Certificate> certsInStates(Set<TrustState> states);
}
