/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.util.Set;

/**
 * <p>Manages the trust managing entity (i.e. CO) part of the shared-state protocol for managing trust.
 * See {@link TrustController2} for the full state machine.
 */
public interface TrustManager {
    Set<X509Certificate> requiringTrust();

    // TODO Does this need to be batched
    void haveTrust(Set<X509Certificate> certs);

    Set<X509Certificate> zombies();

    // TODO Does this need to be batched
    void delete(Set<X509Certificate> certs);
}
