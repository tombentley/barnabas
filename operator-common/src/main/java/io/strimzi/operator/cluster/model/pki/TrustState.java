/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

/**
 * Enumerates the possible states of a set of relying parties' relationship with a given
 * trust root.
 * @see TrustController which defines a state machine using these states
 */
public enum TrustState {
    // logically there's also a HOLD state (from CRL) and a REVOKED state.
    // And when using an external CA we can't assume that a CRL or OCSP aren't being used.

    UNKNOWN("UNKNOWN"),

    /**
     * <p>This is the start state.
     * The certificate is not yet trusted everywhere it needs to be (it may be trusted in some components, but not all).
     */
    UNTRUSTED("UNTRUSTED"),

    /**
     * <p>The certificate is trusted everywhere it needs to be, but end entity certificates signed with is have not yet been deployed.</p>
     */
    TRUSTED_UNUSED("TRUSTED_UNUSED"),

    /**
     * <p>The certificate is trusted everywhere it needs to be, and end entity certificates signed with is have been deployed.</p>
     */
    TRUSTED_USED("TRUSTED_USED"),

    /* The cert is trusted and  and end entity certificates signed with is have been deployed, but
       it's been annotated to be replaced or renewed
     */
    PHASE_OUT("PHASE_OUT"),

    /**
     * <p>The cert is still trusted by at least some components, but is not relied on by any party.
     * One a certificate is in the ZOMBIE state it maby be deletedfrom trust stores. This is the end state.
     */
    ZOMBIE("ZOMBIE");

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
            case "UNKNOWN":
                return UNKNOWN;
            case "UNTRUSTED":
                return UNTRUSTED;
            case "TRUSTED_NEW":
                return TRUSTED_UNUSED;
            case "TRUSTED_USED":
                return TRUSTED_USED;
            case "PHASE_OUT":
                return PHASE_OUT;
            case "ZOMBIE":
                return ZOMBIE;
            default:
                throw new IllegalArgumentException(id);
        }
    }

}
