/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class PkiUtilsTest {

    private byte[] certBytes() throws IOException {
        InputStream resourceAsStream = TrustManagerTest.class.getResourceAsStream("/CLUSTER_CERT.pem");
        return resourceAsStream.readAllBytes();
    }

    @Test
    void x509Certificate() throws CertificateException, IOException {
        byte[] buffer = certBytes();
        X509Certificate x509Certificate = PkiUtils.x509Certificate(buffer);
        assertEquals("CN=cluster-ca, O=Default Company Ltd, L=Default City, C=XX", x509Certificate.getIssuerDN().toString());
        assertNotEquals(-1, x509Certificate.getBasicConstraints(),
                "expected a certificate with CA:true");
        assertEquals(ZonedDateTime.of(2018, 8, 23, 16, 19, 54, 0, ZoneId.of("UTC")).toInstant(), x509Certificate.getNotBefore().toInstant());
        assertEquals(ZonedDateTime.of(2118, 7, 30, 16, 19, 54, 0, ZoneId.of("UTC")).toInstant(), x509Certificate.getNotAfter().toInstant());

    }

    @Test
    public void testAsPem() throws CertificateException, IOException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        try (InputStream is = getClass().getResourceAsStream("/CLIENTS_CERT.pem")) {
            X509Certificate certificate = (X509Certificate) factory.generateCertificate(is);
            try (ByteArrayInputStream in = new ByteArrayInputStream(PkiUtils.asPemString(certificate).getBytes(StandardCharsets.ISO_8859_1))) {
                X509Certificate certificate2 = (X509Certificate) factory.generateCertificate(in);
                assertEquals(certificate, certificate2);
            }
        }
    }

    @Test
    public void testAsPemCertChain() throws CertificateException, IOException {
        String pemStr = TestUtils.readResource(getClass().getResourceAsStream("/cert_chain.pem"));
        byte[] pemBytes = getClass().getResourceAsStream("/cert_chain.pem").readAllBytes();
        List<X509Certificate> x509Certificates = PkiUtils.parsePemCertChain(pemBytes);
        assertEquals(pemStr, PkiUtils.asPemCertChain(x509Certificates));
    }

    @Test
    public void testAsPemCertChainWithInvalidChain() throws IOException {
        byte[] pemBytes = getClass().getResourceAsStream("/invalid_cert_chain.pem").readAllBytes();
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> PkiUtils.parsePemCertChain(pemBytes));
        assertEquals("Certificates do not form a chain: The certificate at index 1 was issued by CN=ISRG Root X1, O=Internet Security Research Group, C=USBut the certificate at index 2 has subject CN=clients-ca, O=Default Company Ltd, L=Default City, C=XX",
                runtimeException.getMessage());

    }

    @Test
    public void testParseCertChain() throws CertificateException, IOException {
        List<X509Certificate> x509Certificates = PkiUtils.parsePemCertChain(getClass().getResourceAsStream("/cert_chain.pem").readAllBytes());
        assertEquals(3, x509Certificates.size());
        assertEquals("CN=*.strimzi.io", x509Certificates.get(0).getSubjectDN().toString());
        assertEquals("CN=R3, O=Let's Encrypt, C=US", x509Certificates.get(1).getSubjectDN().toString());
        assertEquals("CN=ISRG Root X1, O=Internet Security Research Group, C=US", x509Certificates.get(2).getSubjectDN().toString());
    }
}