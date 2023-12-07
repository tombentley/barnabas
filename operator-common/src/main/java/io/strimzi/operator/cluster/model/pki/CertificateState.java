/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

/**
 * Represents the states of a state machine for a particular "End-Entity certificate" (as opposed to CA certificate).
 */
public enum CertificateState {

    /**
     * A certificate for the subject is needed, but doesn't exist in any state.
     * This is the initial state.
     * <p>Transitions to:</p>
     * <dl>
     * <dt>REQUESTED</dt> <dd>Once it's been requested.</dd>
     * </dl>
     */
    REQUIRED("REQUIRED"),

    /**
     * A certificate for the subject has been requested from the {@link CertificateIssuer}.
     * <p>Transitions to:</p>
     * <dl>
     * <dt>IN_USE</dt>     <dd>If, once issued, the certificate can be used right away.</dd>
     * <dt>PENDING</dt>    <dd>If, once issued, the certificate can't be used because the root CA is not yet trusted.</dd>
     * <dt>NOT_NEEDED</dt> <dd>If, by the time it's issued, the certificate is not needed any more.</dd>
     * </dl>
     */
    REQUESTED("REQUESTED"),

    /**
     * A certificate for the subject has been issued by the {@link CertificateIssuer},
     * but the root CA is not trusted yet.
     * <p>Transitions to:</p>
     * <dl>
     * <dt>IN_USE</dt>      <dd>Once the root CA is trusted.</dd>
     * <dt>NOT_NEEDED</dt>  <dd>If the certificate is not needed before it's available for use.</dd>
     * </dl>
     */
    PENDING("PENDING"),

    /**
     * A certificate for the subject is in use.
     * <p>Transitions to:</p>
     * <dl>
     * <dt>REQUESTED</dt>   <dd>If it, or any cert on the path to the trust root, near expiry, or otherwise needs to be replaced.</dd>
     * <dt>NOT_NEEDED</dt>  <dd>If it is not needed before it expires.</dd>
     * </dl>
     */
    IN_USE("IN_USE"),

    /**
     * Not certificate for the subject is required any more.
     * This is the end state.
     */
    NOT_NEEDED("NOT_NEEDED");

    private final String id;

    CertificateState(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static CertificateState fromId(String id) {
        switch (id) {
            case "NEW":
                return REQUIRED;
            case "REQUESTED":
                return REQUESTED;
            case "PENDING":
                return PENDING;
            case "IN_USE":
                return IN_USE;
            case "NOT_NEEDED":
                return NOT_NEEDED;
            default:
                throw new IllegalArgumentException(id);
        }
    }

    @Override
    public String toString() {
        return id;
    }

}
