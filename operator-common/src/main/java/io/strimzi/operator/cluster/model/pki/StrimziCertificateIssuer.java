/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.operator.cluster.model.pki.TrustManager.x509Certificate;
import static io.strimzi.operator.common.Util.delete;

public class StrimziCertificateIssuer implements CertificateIssuer {

    private static final Logger logger = LogManager.getLogger(StrimziCertificateIssuer.class);

    private final CertManager certManager;
    // CA cert and key for the issuing CA
    private final CertAndKey issuingCertAndKey;
    // the cert chain between issuer and root (excluding both)
    private final List<X509Certificate> chain;
    private final Instant min;
    // the trust root
    private X509Certificate trustRoot;

    StrimziCertificateIssuer(CertManager certManager, CertAndKey issuingCaCertAndKey, List<X509Certificate> pathToRoot) {
        this.certManager = certManager;
        this.issuingCertAndKey = issuingCaCertAndKey;

        // This is probably not needed if we do full cert path validation.
        X509Certificate prev = null;
        for (int i = pathToRoot.size() - 1; i >=0; i--) {
            X509Certificate current = pathToRoot.get(i);
            if (current.getBasicConstraints() == -1) {
                throw new IllegalArgumentException("The chain of CA certificate contains a non-CA certificate: " + current.getSubjectDN());
            }
            if (prev != null) {
                if (!current.getIssuerDN().toString().equals(prev.getSubjectDN().toString())) {
                    throw new IllegalArgumentException("pathToRoot should start with a CA certificate and each next " +
                            "certificate's issued should be the previous certificate's subject: ");
                }
            }
            prev = current;
        }

        // validate that the root CA is really a root of the issuing CA
        X509Certificate issuingCert;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");

            ArrayList<X509Certificate> unorderedChain = new ArrayList<>();
            issuingCert = x509Certificate(issuingCaCertAndKey.cert());
            unorderedChain.add(issuingCert);
            unorderedChain.addAll(pathToRoot.subList(1, pathToRoot.size()));

            CertPathValidator validator = CertPathValidator.getInstance("PKIX");
            PKIXParameters parameters = new PKIXParameters(Set.of(new TrustAnchor(pathToRoot.get(0), null)));
            parameters.setRevocationEnabled(false);
            CertPath certPath = cf.generateCertPath(unorderedChain);
            PKIXCertPathValidatorResult validate = (PKIXCertPathValidatorResult) validator.validate(certPath, parameters);
            trustRoot = validate.getTrustAnchor().getTrustedCert();
            chain = new ArrayList<>((List<X509Certificate>) certPath.getCertificates());
            chain.remove(issuingCert);
            Instant min = issuingCert.getNotAfter().toInstant();
            for (X509Certificate c : chain) {
                Instant instant = c.getNotAfter().toInstant();
                min = min(min, instant);
            }
            Instant instant = trustRoot.getNotAfter().toInstant();
            min = min(min, instant);
            this.min = min;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
        // TODO validate that the issuing ca cert and key are a pair
    }

    private Instant min(Instant min, Instant instant) {
        if (instant.isBefore(min)) {
            min = instant;
        }
        return min;
    }

    /**
     * Create a new root CA cert and key using the given {@code certManager}.
     */
    public static StrimziCertificateIssuer createRootIssuer(CertManager certManager, Subject subject, int days) throws CertificateException {
        logger.debug("Creating StrimziCertificateIssuer with CA subject {} and root cert validity {}", subject, days);
        Path keyPath = null;
        Path certPath = null;
        byte[] key;
        byte[] cert;
        try {
            try {
                keyPath = Files.createTempFile(null, ".key");
                certPath = Files.createTempFile(null, ".crt");
                certManager.generateSelfSignedCert(keyPath.toFile(), certPath.toFile(), subject, days);
                cert = Files.readAllBytes(certPath);
                key = Files.readAllBytes(keyPath);
            } catch (IOException e) {
                throw new CaCreationFailed(e);
            }
        } finally {
            if (keyPath != null) {
                delete(keyPath);
            }
            if (certPath != null) {
                delete(certPath);
            }
        }
        return new StrimziCertificateIssuer(certManager, new CertAndKey(key, cert), List.of(x509Certificate(cert)));
        // TODO when the issuing CA is self-signed, do we include it in the chain?
    }

    public IssuedCertificate issue(Subject subject, int requestedValidity) throws CertificateIssuanceException {
        try {
            int actualValidity = adjustValidity(requestedValidity);
            logger.debug("Issuing certificate: subject: {}, requestedValidity: {}, actualValidity: {}", subject, requestedValidity, actualValidity);

            byte[] issuedKey;
            Path csrPath = Files.createTempFile(null, ".csr");
            {
                Path issuedKeyPath = Files.createTempFile(null, ".csr");
                try {
                    certManager.generateCsr(issuedKeyPath.toFile(), csrPath.toFile(), subject);
                    issuedKey = Files.readAllBytes(issuedKeyPath);
                } finally {
                    delete(issuedKeyPath);
                }
            }

            byte[] issuedCert;
            Path caKeyPath = Files.createTempFile("ca", ".key");
            Path caCertPath = Files.createTempFile("ca", ".crt");
            try {
                Files.write(caKeyPath, issuingCertAndKey.key());
                Files.write(caCertPath, issuingCertAndKey.cert());
                Path issuedCertPath = Files.createTempFile(null, ".csr");
                certManager.generateCert(csrPath.toFile(), caKeyPath.toFile(), caCertPath.toFile(), issuedCertPath.toFile(), subject, actualValidity);
                issuedCert = Files.readAllBytes(issuedCertPath);
            } finally {
                delete(caKeyPath);
                delete(caCertPath);
                delete(csrPath);
            }

            return new IssuedCertificate(issuedKey, x509Certificate(issuedCert), chain, trustRoot);
        } catch (IOException | CertificateException e) {
            throw new CertificateIssuanceException(e);
        }
    }

    private int adjustValidity(int requestedValidity) throws CertificateException {
        // TODO probably want to use the minimum date of the whole chain
        X509Certificate issuingCert = x509Certificate(issuingCertAndKey.cert());
        Instant now = Instant.now();
        Instant requestedEnd = now.plus(requestedValidity, ChronoUnit.DAYS);
        Instant actualEnd = min(requestedEnd, this.min);
        long actualValidity = Duration.between(now, actualEnd).toDays();
        int actualValidityInt;
        if (actualValidity > Integer.MAX_VALUE) {
            throw new RuntimeException();
        } else {
            actualValidityInt = (int) actualValidity;
        }
        return actualValidityInt;
    }

    public byte[] issuingKey() {
        return issuingCertAndKey.key();
    }

    public byte[] issuingCert() {
        return issuingCertAndKey.cert();
    }
}
