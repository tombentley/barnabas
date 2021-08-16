/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

/**
 * Abstraction for things which can store {@link CertificateStatus CertificateStatuses}
 * and deploy {@code IssuedCertificate IssuedCertificates} for use by components/clients.
 * This includes, but is not limited to, putting them in a Secret.
 */
public interface CertificateOutputWriter<I extends SubjectIdentity> {
    /**
     * Load the certificate status for the given {@code subjectIdentity}
     * @param subjectIdentity The subject
     * @return Its status. If the given {@code subjectIdentity} doesn't have a status then a new one in
     * state {@link CertificateState#NEW} should be returned.
     */
    CertificateStatus loadStatus(I subjectIdentity);

    /**
     * Store the given {@code certificateStatus} for later retrieval by {@link #loadStatus(SubjectIdentity)}.
     * @param subjectIdentity The subject
     * @param issuedCertificate The pending certificate
     */
    void storePending(I subjectIdentity, IssuedCertificate issuedCertificate);

    void storeRequested(I subjectIdentity, byte[] token);

    /**
     * Write the given {@code certificate} that was issued for the given {@code subjectIdentity}
     * @param subjectIdentity The identity
     * @param certificate The certificate
     * @throws CertificateException Problem with cert
     * @throws InvalidKeySpecException Problem with key
     * @throws KeyStoreException PRoblem storing key
     * @throws IOException Problem with IO
     */
    void writeIssued(I subjectIdentity,
                     IssuedCertificate certificate)
            throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException;
}
