/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

/**
 * Enumerates the states of trust relationship with a CA certificate.
 */
public enum CaCertState {
    /**
     * <p>This is the start state.
     * The certificate is not yet trusted everywhere it needs to be (it may be trusted in some components, but not all).
     * It may not be used to sign certificates.
     *
     * <p>Transitions to:</p>
     * <ul>
     * <li>{@link #TRUSTED} once all the pods have been restarted (cluster CA or clients CA).</li>
     * <li>{@link #ZOMBIE} if the certificate expires or is marked for renewal before all the pods have been restarted.</li>
     * </ul>
     */
    UNTRUSTED,

    /**
     * <p>The certificate is trusted everywhere it needs to be.
     * It may be used to sign certificates.
     * A certificate transitions to this state when
     * all the pods which will observe client certificates signed by this certificate
     * have been restarted (cluster CA or clients CA).</p>
     *
     * <p>Transitions to:</p>
     * <ul>
     * <li>{@link #PHASE_OUT} when any renewable certificate in the chain to the trust root will
     * expire within the renewal period, or it is marked for renewal.</li>
     * </ul>
     */
    TRUSTED,

    /**
     * <p>The certificate is trusted everywhere it needs to be and is still relied upon in cert paths.
     * It may not be used to sign certificates.
     * A certificate transitions to this state when
     * any renewable certificate in the chain to the trust root will
     * expire within the renewal period, or it is marked for renewal.</p>
     *
     * <p>Transitions to:</p>
     * <ul>
     *     <li>{@link #ZOMBIE} when no certificate signed by this CA certificate is in active use.
     *     For the cluster CA this can be determined automatically.
     *     For the clients CA some manual action would be required to tell the CO that this is the case.</li>
     * </ul>
     */
    PHASE_OUT,

    /**
     * <p>The cert is still trusted by at least some components, but is not relied on by clients.
     * Zombie certificates can be removed from truststores.
     * A certificate transitions to this state when no certificate signed by this CA certificate is in active use.
     * </p>
     *
     * <p>Transitions to:</p>
     * <ul>
     *     <li>Dead once it is no longer trusted by anything.</li>
     * </ul>
     */
    ZOMBIE
}
