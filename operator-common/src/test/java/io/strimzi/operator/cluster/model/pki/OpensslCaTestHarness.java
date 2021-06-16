/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;

public abstract class OpensslCaTestHarness {
    protected final OpenSslCertManager openssl = new OpenSslCertManager();

    protected IssuedCertificate issuedCertificate() throws IOException, CertificateException {

        Path caKey = Files.createTempFile("", "");
        Path caCert = Files.createTempFile("", "");
        ZonedDateTime now = Instant.now().atZone(ZoneId.of("UTC"));
        openssl.generateRootCaCert(new Subject.Builder().withCommonName("My Root CA").build(), caKey.toFile(), caCert.toFile(),
                now, now.plus(1, ChronoUnit.YEARS), 1);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X509");
        X509Certificate rootCert = (X509Certificate) certificateFactory.generateCertificate(new FileInputStream(caCert.toFile()));

        Path intermediateKey = Files.createTempFile("", "");
        Path intermediateCert = Files.createTempFile("", "");
        openssl.generateIntermediateCaCert(caKey.toFile(), caCert.toFile(),
                new Subject.Builder().withCommonName("My Intermediate CA").build(),
                intermediateKey.toFile(), intermediateCert.toFile(),
                now, now.plus(1, ChronoUnit.DAYS), 0);

        Path endEntityKey = Files.createTempFile("", "");
        Path csr = Files.createTempFile("", "");
        openssl.generateCsr(endEntityKey.toFile(), csr.toFile(),
                new Subject.Builder().withCommonName("My Entity Cert").build());

        Path eeCert = Files.createTempFile("", "");
        openssl.generateCert(csr.toFile(), intermediateKey.toFile(), intermediateCert.toFile(), eeCert.toFile(),
                new Subject.Builder().withCommonName("My Entity Cert").build(), now, now.plus(1, ChronoUnit.YEARS));

        return new IssuedCertificate(Files.readAllBytes(endEntityKey),
                PkiUtils.x509Certificate(eeCert),
                List.of(PkiUtils.x509Certificate(intermediateCert)), PkiUtils.x509Certificate(caCert));
    }
}
