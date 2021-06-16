/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

/**
 * Enumerates the possible states of a set of relying parties' relationship with a given
 * CA certificate.
 */
public enum TrustState {
    // logically there's also a HOLD state (from CRL) and a REVOKED state.
    // And when using an external CA we can't assume that a CRL or OCSP aren't being used.

    UNKNOWN("unknown"),

    /**
     * <p>This is the start state.
     * The certificate is not yet trusted everywhere it needs to be (it may be trusted in some components, but not all).
     * It may not be used to sign certificates.
     *
     * <p>Transitions to:</p>
     * <ul>
     * <li>{@link #TRUSTED} once all parties trust this CA certificate.</li>
     * <li>{@link #ZOMBIE} if this CA certificate expires or is marked for renewal before all the pods have been restarted.</li>
     * </ul>
     */
    UNTRUSTED("untrusted"),

    /**
     * <p>The certificate is trusted everywhere it needs to be.
     * It may be used to sign certificates and thus is relied upon in cert paths.</p>
     *
     * <p>A CA certificate transitions to this state when all relying parties trust it:</p>
     * <dl>
     *     <dt>For the cluster CA</dt>
     *     <dd>user applications need to trust cluster CA certificates, so we can't transition to TRUSTED
     *     until we know that user applications now trust this CA certificate.</dd>
     *
     *     <dt>For the clients CA</dt>
     *     <dd>Kafka brokers need to trust client certificates signed by client CA certificates,
     *     so can transition to TRUSTED as soon as those brokers have that trust (e.g. have been restarted).</dd>
     * </dl>
     *
     * <p>Transitions to: {@link #PHASE_OUT}.</p>
     */

    TRUSTED("trusted"),

    /**
     * <p>The certificate is trusted everywhere it needs to be and may still relied upon in cert paths.
     * It may not be used to sign new client certificates.</p>
     *
     * <p>A certificate transitions to this state when any renewable certificate in the chain to the trust root will
     * expire within the renewal period, or it is explicitly marked for renewal.</p>
     *
     * <p>Client certificates which were signed by a CA certificate which is in PHASE_OUT should be actively
     * replaced:</p>
     * <dl>
     *     <dt>cluster CA</dt>
     *     <dd>the client certificate signed by a cluster CA certificate are all internal to Strimzi, so the client
     *     certificate replacement can be done automatically, by the operator.</dd>
     *     <dt>clients CA</dt>
     *     <dd>We don't know that all the client certificates signed with a clients CA certificate are created by the
     *     User Operator, so we have to be told by the user.
     * </dl>
     *
     * <p>Transitions to {@link #ZOMBIE}.</p>
     */
    PHASE_OUT("phase-out"),

    /**
     * <p>The cert is still trusted by at least some components, but is not relied on by any party.
     * One a certificate is in the ZOMBIE state it will be deleted from Secrets and removed from trust stores. This is the end state.
     *
     * <p>A certificate transitions to this state when no certificates signed by this CA certificate are in active use:</p>
     * <dl>
     *     <dt>cluster CA</dt>
     *     <dd>the client certificate signed by a cluster CA certificate are all internal to Strimzi, so the client
     *     certificate replacement can be done automatically by the operator once a CA certificate enters the
     *     PHASE_OUT state and once all those newly issued certificates are in use the transition to ZOMBIE can be
     *     done automatically.</dd>
     *
     *     <dt>clients CA</dt>
     *     <dd>We don't know that all extant client certificates signed with a clients CA certificate are created by the
     *     User Operator, so we have to be told by the user. (Even if the UO issued new client certs for every
     *     KafkaUser we don't know that those are actually being used by all the applications).
     *     So transition to ZOMBIE is gated on WHAT????
     * </dl>
     */
    ZOMBIE("zombie");

    private final String id;

    TrustState(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }

    public static TrustState fromId(String id) {
        Objects.requireNonNull(id);
        switch (id) {
            case "unknown":
                return UNKNOWN;
            case "untrusted":
                return UNTRUSTED;
            case "trusted":
                return TRUSTED;
            case "phase-out":
                return PHASE_OUT;
            case "zombie":
                return ZOMBIE;
            default:
                throw new IllegalArgumentException();
        }
    }

}
