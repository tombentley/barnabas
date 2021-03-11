/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class IssuedCertificateTest {

    @Test
    void pkcs12RoundTrip() throws InvalidKeySpecException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, UnrecoverableEntryException {
        Subject caSubject = new Subject();
        caSubject.setCommonName("test ca");
        StrimziCertificateIssuer rootIssuer = StrimziCertificateIssuer.createRootIssuer(new OpenSslCertManager(), caSubject, 10);
        Subject leafSubject = new Subject();
        leafSubject.setCommonName("leaf ca");
        IssuedCertificate stored = rootIssuer.issue(leafSubject, 10);
        byte[] bytes = stored.toPkcs12("foo".toCharArray());
        IssuedCertificate loaded = IssuedCertificate.fromPkcs12(bytes, "foo".toCharArray());
        // TODO eugh
        assertEquals(new String(stored.privateKey()).replace("\n", ""),
                new String(loaded.privateKey()).replace("\n", ""), "Private keys differ");
        assertEquals(stored.issuedCertificate(), loaded.issuedCertificate(), "Issued certificates differ");
        assertEquals(stored.intermediateChain(), loaded.intermediateChain(), "Intermediate certificate chains differ");
        assertNull(loaded.rootCaCertificate(), "Root CA certificate should be null");
    }
}