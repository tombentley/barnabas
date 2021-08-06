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

    UNKNOWN("unknown"),

    /**
     * <p>This is the start state.
     * The certificate is not yet trusted everywhere it needs to be (it may be trusted in some components, but not all).
     */
    UNTRUSTED("untrusted"),

    /**
     * <p>The certificate is trusted everywhere it needs to be, but end entity certificates signed with is have not yet been deployed.</p>
     */
    TRUSTED_NEW("trusted_new"),

    /**
     * <p>The certificate is trusted everywhere it needs to be, and end entity certificates signed with is have been deployed.</p>
     */
    TRUSTED_USED("trusted_used"),


    /**
     * <p>The cert is still trusted by at least some components, but is not relied on by any party.
     * One a certificate is in the ZOMBIE state it maby be deletedfrom trust stores. This is the end state.
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
            case "trusted_new":
                return TRUSTED_NEW;
            case "trusted_used":
                return TRUSTED_USED;
            case "zombie":
                return ZOMBIE;
            default:
                throw new IllegalArgumentException(id);
        }
    }

}
