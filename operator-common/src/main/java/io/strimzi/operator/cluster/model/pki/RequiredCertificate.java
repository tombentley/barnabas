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
    public static final String PENDING_SUFFIX = ".pending";
    private final Subject subject;
    private final String secretName;
    private final String entryName;

    public RequiredCertificate(Subject subject, String secretName, String entryName) {
        Objects.requireNonNull(subject);
        Objects.requireNonNull(secretName);
        Objects.requireNonNull(entryName);
        this.subject = subject;
        this.secretName = secretName;
        this.entryName = entryName;
    }

    /**
     * @return The subject of the certificate
     */
    public Subject subject() {
        return subject;
    }

    /**
     * @return The name of the Secret where the certificate should eventually be stored
     */
    public String secretName() {
        return secretName;
    }

    /**
     * @return The name of the entry in the {@link #secretName()} Secret which contains the certificate chain.
     */
    public String certChainEntryName() {
        return entryName;
    }

    public String pendingCertChainEntryName() {
        return certChainEntryName() + PENDING_SUFFIX;
    }

    @Override
    public String toString() {
        return "RequiredCertificate(" +
                "subject=" + subject +
                ", secretName='" + secretName + '\'' +
                ", entryName='" + entryName + '\'' +
                ')';
    }
}
