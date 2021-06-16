/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

import io.strimzi.certs.Subject;

/**
 * Represents a certificate which is required for something to function.
 * For example this could be a certificate for a given broker.
 */
public class RequiredCertificate {

    private final SubjectIdentity subjectIdentity;
    private final Subject subject;

    public RequiredCertificate(SubjectIdentity subjectIdentity, Subject subject) {
        this.subjectIdentity = Objects.requireNonNull(subjectIdentity);
        this.subject = Objects.requireNonNull(subject);
    }

    /**
     * @return The subject and SAN of the certificate.
     */
    public Subject subjectAndSan() {
        return subject;
    }

    /**
     * @return The subject's DN.
     */
    public SubjectIdentity subjectIdentity() {
        return subjectIdentity;
    }

    @Override
    public String toString() {
        return "RequiredCertificate(" +
                "subjectIdentity=" + subjectIdentity +
                ", subject=" + subject +
                ')';
    }
}
