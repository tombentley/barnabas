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
import java.util.List;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class PemTest {
    @Test
    public void testAsPem() throws CertificateException, IOException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        try (InputStream is = getClass().getResourceAsStream("/CLIENTS_CERT.pem")) {
            X509Certificate certificate = (X509Certificate) factory.generateCertificate(is);
            try (ByteArrayInputStream in = new ByteArrayInputStream(Pem.asPem(certificate).getBytes(StandardCharsets.ISO_8859_1))) {
                X509Certificate certificate2 = (X509Certificate) factory.generateCertificate(in);
                assertEquals(certificate, certificate2);
            }
        }
    }

    @Test
    public void testAsPemCertChain() throws CertificateException {
        String pemStr = TestUtils.readResource(getClass().getResourceAsStream("/cert_chain.pem"));
        List<X509Certificate> x509Certificates = Pem.parseCertChain(pemStr);
        assertEquals(pemStr, Pem.asPemCertChain(x509Certificates));
    }

    @Test
    public void testAsPemCertChainWithInvalidChain() {
        String pemStr = TestUtils.readResource(getClass().getResourceAsStream("/invalid_cert_chain.pem"));
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> Pem.parseCertChain(pemStr));
        assertEquals("Certificates do not form a chain: The certificate at index 1 was issued by CN=ISRG Root X1, O=Internet Security Research Group, C=USBut the certificate at index 2 has subject CN=clients-ca, O=Default Company Ltd, L=Default City, C=XX",
                runtimeException.getMessage());

    }

    @Test
    public void testParseCertChain() throws CertificateException {
        List<X509Certificate> x509Certificates = Pem.parseCertChain(TestUtils.readResource(getClass().getResourceAsStream("/cert_chain.pem")));
        assertEquals(3, x509Certificates.size());
        assertEquals("CN=*.strimzi.io", x509Certificates.get(0).getSubjectDN().toString());
        assertEquals("CN=R3, O=Let's Encrypt, C=US", x509Certificates.get(1).getSubjectDN().toString());
        assertEquals("CN=ISRG Root X1, O=Internet Security Research Group, C=US", x509Certificates.get(2).getSubjectDN().toString());
    }
}