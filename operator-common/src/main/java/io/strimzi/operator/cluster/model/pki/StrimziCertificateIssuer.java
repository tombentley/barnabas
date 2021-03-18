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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.operator.common.Util.delete;

public class StrimziCertificateIssuer implements CertificateIssuer {

    private static final Logger LOGGER = LogManager.getLogger(StrimziCertificateIssuer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Clock clock;
    private final CertManager certManager;
    // CA cert and key for the issuing CA
    //private final CertAndKey issuingCertAndKey;
    // the cert chain between issuer and root (excluding both)
    private final List<X509Certificate> chain;
    /** The soonest notAfter date of all the certificate on the path from the issuing cert to the trust root*/
    private final Instant max;
    private final X509Certificate issuingCert;
    private final byte[] issuingKey;
    // the trust root
    private final X509Certificate trustRoot;

    public static class Token {

        private final byte[] key;
        private final byte[] csr;
        private final int requestedValidityDays; // TODO should we support smaller units (e.g. minutes)
        private final Subject subject;

        @JsonCreator
        public Token(
                @JsonProperty("key") byte[] key,
                @JsonProperty("csr") byte[] csr,
                @JsonProperty("requestedValidityDays") int requestedValidityDays,
                @JsonProperty("subject") Subject subject) {
            this.key = key;
            this.csr = csr;
            this.requestedValidityDays = requestedValidityDays;
            this.subject = subject;
        }

        @JsonProperty
        public byte[] key() {
            return key;
        }

        @JsonProperty
        public byte[] csr() {
            return csr;
        }

        @JsonProperty
        public int requestedValidityDays() {
            return requestedValidityDays;
        }

        @JsonProperty
        public Subject subject() {
            return subject;
        }
    }

    @SuppressWarnings("unchecked")
    StrimziCertificateIssuer(Clock clock, CertManager certManager, byte[] issuingKey, X509Certificate issuingCert, List<X509Certificate> pathToRoot) {
        this.clock = clock;
        this.certManager = certManager;
        this.issuingCert = issuingCert;
        this.issuingKey = issuingKey;

        // This is probably not needed if we do full cert path validation.
        X509Certificate prev = null;
        for (int i = pathToRoot.size() - 1; i >= 0; i--) {
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
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");

            ArrayList<X509Certificate> unorderedChain = new ArrayList<>();
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
            Instant max = issuingCert.getNotAfter().toInstant();
            for (X509Certificate c : chain) {
                Instant instant = c.getNotAfter().toInstant();
                max = min(max, instant);
            }
            Instant instant = trustRoot.getNotAfter().toInstant();
            max = min(max, instant);
            this.max = max;
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
     * @param certManager The cert manager to use.
     * @param subject The subject for the root CA certificate.
     * @param validityDays The validity period for the root CA certificate.
     * @return An issuer instance.
     */
    public static StrimziCertificateIssuer createRootIssuer(Clock clock, CertManager certManager, Subject subject, int validityDays) {
        LOGGER.debug("Creating StrimziCertificateIssuer with CA subject {} and root cert validity {}", subject, validityDays);
        Path keyPath = null;
        Path certPath = null;
        byte[] key;
        X509Certificate cert;
        try {
            try {
                keyPath = Files.createTempFile(null, ".key");
                certPath = Files.createTempFile(null, ".crt");
                certManager.generateSelfSignedCert(keyPath.toFile(), certPath.toFile(), subject, validityDays);
                cert = PkiUtils.x509Certificate(certPath);
                key = Files.readAllBytes(keyPath);
            } catch (IOException | CertificateException e) {
                throw new CaCreationFailed(e);
            }
        } finally {
            delete(keyPath);
            delete(certPath);
        }
        return new StrimziCertificateIssuer(clock, certManager, key, cert, List.of(cert));
        // TODO when the issuing CA is self-signed, do we include it in the chain?
    }


    private int adjustValidity(int requestedValidity) {
        // TODO probably want to use the minimum date of the whole chain
        Instant now = clock.instant();
        Instant requestedEnd = now.plus(requestedValidity, ChronoUnit.DAYS);
        Instant actualEnd = min(requestedEnd, this.max);
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
        return issuingKey;
    }

    public X509Certificate issuingCert() {
        return issuingCert;
    }

    @Override
    public byte[] requestCertificate(Subject subject, int requestedValidityDays) throws CertificateIssuanceException {
        try {
            byte[] issuedKey;
            byte[] csr;
            Path csrPath;
            Path issuedKeyPath = Files.createTempFile(null, ".csr");
            try {
                csrPath = Files.createTempFile(null, ".csr");
                issuedKeyPath = Files.createTempFile(null, ".csr");
                certManager.generateCsr(issuedKeyPath.toFile(), csrPath.toFile(), subject);
                issuedKey = Files.readAllBytes(issuedKeyPath);
                csr = Files.readAllBytes(csrPath);
            } finally {
                delete(issuedKeyPath);
            }
            return MAPPER.writeValueAsBytes(new Token(issuedKey, csr, requestedValidityDays, subject));
        } catch (IOException e) {
            throw new CertificateIssuanceException(e);
        }
    }

    @Override
    public Optional<IssuedCertificate> pollForRequestedCertificate(byte[] tokenBytes) throws CertificateIssuanceException {
        try {
            Token token = MAPPER.readValue(tokenBytes, Token.class);
            int actualValidity = adjustValidity(token.requestedValidityDays());
            LOGGER.debug("Issuing certificate: subject: {}, requestedValidity: {}, actualValidity: {}", token.subject(), token.requestedValidityDays(), actualValidity);

            X509Certificate issuedCertificate;

            Path csrPath = null;
            Path caKeyPath = null;
            Path caCertPath = null;
            Path issuedCertPath = null;
            try {
                csrPath = Files.createTempFile(null, null);
                caKeyPath = Files.createTempFile(null, null);
                caCertPath = Files.createTempFile(null, null);
                issuedCertPath = Files.createTempFile(null, null);
                Files.write(csrPath, token.csr());
                Files.write(caKeyPath, issuingKey);
                PkiUtils.writePem(caCertPath, issuingCert);
                certManager.generateCert(csrPath.toFile(), caKeyPath.toFile(), caCertPath.toFile(), issuedCertPath.toFile(), token.subject(), actualValidity);
                issuedCertificate = PkiUtils.x509Certificate(issuedCertPath);
            } finally {
                delete(caKeyPath);
                delete(caCertPath);
                delete(csrPath);
                delete(issuedCertPath);
            }


            return Optional.of(new IssuedCertificate(token.key(), issuedCertificate, chain, trustRoot));
        } catch (IOException | CertificateException e) {
            throw new CertificateIssuanceException(e);
        }
    }
}
