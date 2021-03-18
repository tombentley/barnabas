/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.temporal.ChronoUnit;

import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import org.junit.jupiter.api.Test;

import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class StrimziCertificateIssuerTest {

    @Test
    void simpleCa() throws CertificateException {
        Subject caSubject = new Subject.Builder()
            .withCommonName("my root CA").build();
        StrimziCertificateIssuer issuer = StrimziCertificateIssuer.createRootIssuer(Clock.systemUTC(), new OpenSslCertManager(), caSubject, 10);
        Subject clientSubject = new Subject.Builder()
            .withCommonName("my client").build();
        byte[] token = issuer.requestCertificate(clientSubject, 100);
        IssuedCertificate issuance = issuer.pollForRequestedCertificate(token).get();
        X509Certificate issuedCertificate = issuance.issuedCertificate();
        assertEquals("CN=my client", issuedCertificate.getSubjectDN().toString());
        assertEquals("CN=my root CA", issuedCertificate.getIssuerDN().toString());
        assertEquals(-1, issuedCertificate.getBasicConstraints(),
                "Expected a cert without CA:true");

        assertTrue(Math.abs(now().getEpochSecond() - issuedCertificate.getNotBefore().toInstant().getEpochSecond()) < 10L,
                "Expect issued cert to start right now (roughly)");
        assertTrue(now().plus(10, ChronoUnit.DAYS).getEpochSecond() > issuedCertificate.getNotAfter().toInstant().getEpochSecond(),
                "Expect the issued cert to finish about 10 days from now");

        assertTrue(issuance.intermediateChain().isEmpty(),
                "Didn't expect a cert chain");
        // TODO assert the returned key is a key for the returned cert
        // TODO assert a cert path from the issued cert + chain to issuer cert
    }

    @Test
    void givenRootCaCert() {
        // TODO
    }

    @Test
    void givenIntermediateCert() {
        // TODO
    }
    // TODO we should check the JSON form of the token, that the byte[] are serialized nicely (i.e. bse64 encoded rather than array of int).
}