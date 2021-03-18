/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Optional;

import io.strimzi.certs.Subject;

/**
 * An abstraction of a Certificate Authority (CA), i.e. something that can issue X.509 certificates.
 * This abstractions supports asynchronous issuance because in general we don't know whether certificate will
 * be issued quickly or not.
 */
interface CertificateIssuer {

    /**
     * Requests a certificate for the given subject with the given {@code validityPeriod}.
     * @param subject The subject of the requested certificate
     * @param requestedValidity The requsted validity period, in days.
     * @return A token which can later be passed to {@link #pollForRequestedCertificate(byte[])}.
     * @throws CertificateIssuanceException
     */
    byte[] requestCertificate(Subject subject, int requestedValidity) throws CertificateIssuanceException;

    /**
     * Polls for the completion of a certificate previously requested using {@link #requestCertificate(Subject, int)}.
     * @param token The token returned by {@link #requestCertificate(Subject, int)}.
     * @return null if the request is still on going, or the issued certificate if the request is completed.
     * @throws CertificateIssuanceException
     */
    Optional<IssuedCertificate> pollForRequestedCertificate(byte[] token) throws CertificateIssuanceException;
}
