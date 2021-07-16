/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import io.fabric8.kubernetes.api.model.OwnerReference;

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
    CertificateStatus<I> loadStatus(I subjectIdentity);

    /**
     * Store the given {@code certificateStatus} for later retrieval by {@link #loadStatus(SubjectIdentity)}.
     * @param certificateStatus
     */
    void storeStatus(CertificateStatus<I> certificateStatus);

    /**
     * Write the given {@code certificate} that was issued for the given {@code subjectIdentity}
     * @param subjectIdentity
     * @param certificate
     * @throws CertificateException
     * @throws InvalidKeySpecException
     * @throws KeyStoreException
     * @throws IOException
     */
    void writeIssued(I subjectIdentity,
                     IssuedCertificate certificate)
            throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException;
}
