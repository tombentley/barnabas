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
 * Abstraction for things which know how to take a bunch of {@code IssuedCertificate IssuedCertificates}
 * and put them somewhere for use by components/clients.
 * This includes, but is not limited to, putting them in a Secret.
 * This is a distinct responsibility from storing a {@link CertificateStatus}.
 */
public interface CertificateOutputWriter {
    void writeIssued(SubjectIdentity subjectIdentity, IssuedCertificate certificate)
            throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException;
}
