/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

/**
 * A certificate that has been issued, but can't be used yet because of a lack of trust in it's
 * trust chain.
 */
public class PendingCertificate {
    private final RequiredCertificate requiredCertificate;
    private final IssuedCertificate issuedCertificate;

    public PendingCertificate(RequiredCertificate requiredCertificate, IssuedCertificate issuedCertificate) {
        this.requiredCertificate = requiredCertificate;
        this.issuedCertificate = issuedCertificate;
    }

    public RequiredCertificate requiredCertificate() {
        return requiredCertificate;
    }

    public IssuedCertificate issuedCertificate() {
        return issuedCertificate;
    }

    @Override
    public String toString() {
        return "PendingCertificate(" +
                "requiredCertificate=" + requiredCertificate +
                ", issuedCertificate=" + issuedCertificate +
                ')';
    }
}
