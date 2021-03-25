/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public enum CertificateState {

    /**
     * A certificate for the subject is needed, but doesn't exist in any state.
     * This is the initial state.
     * Transitions to
     * REQUESTED once it's been requested
     */
    NEW("new"),

    /**
     * A certificate for the subject has been requested from the {@link CertificateIssuer}.
     * Transitions to
     * IN_USE if, once issued, the certificate can be used right away
     * PENDING if, once issued, the certificate can't be used because the root CA is not yet trusted.
     * NOT_NEEDED if, by the time it's issued, the certificate is not needed any more.
     */
    REQUESTED("requested"),

    /**
     * A certificate for the subject has been issued by the {@link CertificateIssuer},
     * but the root CA is not trusted yet.
     * Transitions to
     * IN_USE Once the root CA is trusted
     * NOT_NEEDED If the certificate is not needed before it's available for use.
     */
    PENDING("pending"),

    /**
     * A certificate for the subject is in use.
     * Transitions to:
     * REQUESTED If it, or any cert on the path to the trust root, near expiry, or otherwise needs to be replaced
     * NOT_NEEDED If it is not needed before it expires
     */
    IN_USE("in-use"),

    /**
     * Not certificate for the subject is required any more.
     * This is the end state
     */
    NOT_NEEDED("not-needed");

    private final String id;

    CertificateState(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static CertificateState fromId(String id) {
        switch (id) {
            case "new":
                return NEW;
            case "requested":
                return REQUESTED;
            case "pending":
                return PENDING;
            case "in-use":
                return IN_USE;
            case "not-needed":
                return NOT_NEEDED;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return id;
    }

}
