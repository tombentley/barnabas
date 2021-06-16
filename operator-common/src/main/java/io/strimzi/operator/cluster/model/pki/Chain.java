/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;

import static io.strimzi.operator.common.Util.delete;

/**
 * Constructs an {@link InternalCertificateIssuer} according to a {@link CertificateAuthority}
 * (or more generally, some other API defining how to issue certificates).
 */
public class Chain {

    public static class IssuanceParams {
        private final Subject subject;
        private final int validity;
        private final CertificateExpirationPolicy policy;

        public IssuanceParams(Subject subject, int validity, CertificateExpirationPolicy policy) {
            this.subject = Objects.requireNonNull(subject);
            this.policy = policy;
            if (validity <= 0) {
                throw new IllegalArgumentException();
            }
            this.validity = validity;
        }

        public Subject getSubject() {
            return subject;
        }

        public int getValidity() {
            return validity;
        }

        @Override
        public String toString() {
            return "IssuanceParams(" +
                    "subject=" + subject +
                    ", validity=" + validity +
                    ')';
        }
    }

    public static class CaCertificateReference {
        /**
         * The name of the secret containing the CA cert.
         * Never null.
         */
        private final String certificateSecret;

        /**
         * The name of the item containing the CA cert within the certificate secret.
         * Never null.
         */
        private final String certificateItem;

        /**
         * The name of the secret containing the CA key.
         * May be null (e.g. Root CA keys stored offline).
         */
        private final String keySecret;

        /**
         * The name of the item containing the CA key within the key secret.
         * May be null (e.g. Root CA keys stored offline).
         */
        private final String keySecretItem;

        /**
         * If this certificate can be issued, these are the parameters.
         * Null if this certificate is read only.
         */
        private final IssuanceParams issuanceParams;

        public CaCertificateReference(String certificateSecret, String certificateItem, String keySecret, String keySecretItem, IssuanceParams issuanceParams) {
            this.certificateSecret = Objects.requireNonNull(certificateSecret);
            this.certificateItem = Objects.requireNonNull(certificateItem);
            this.keySecret = keySecret;
            this.keySecretItem = keySecretItem;
            this.issuanceParams = issuanceParams;
        }

        public String getCertificateSecret() {
            return certificateSecret;
        }

        public String getCertificateItem() {
            return certificateItem;
        }

        public String getKeySecret() {
            return keySecret;
        }

        public String getKeySecretItem() {
            return keySecretItem;
        }

        public IssuanceParams getIssuanceParams() {
            return issuanceParams;
        }

        public boolean isIssuable() {
            return getIssuanceParams() != null;
        }

        @Override
        public String toString() {
            return "CaCertificateReference(" +
                    "certificateSecret='" + certificateSecret + '\'' +
                    ", certificateId='" + certificateItem + '\'' +
                    ", keySecret='" + keySecret + '\'' +
                    ", keyId='" + keySecretItem + '\'' +
                    ", issuanceParams=" + issuanceParams +
                    ')';
        }
    }

    public static class InternalIssuerSpec {
        private final int renewalDays;
        private final List<CaCertificateReference> chain;

        public InternalIssuerSpec(int renewalDays, List<CaCertificateReference> chain) {
            this.renewalDays = renewalDays;
            this.chain = chain;
        }

        public int getRenewalDays() {
            return renewalDays;
        }

        public List<CaCertificateReference> getChain() {
            return chain;
        }

        @Override
        public String toString() {
            return "InternalIssuerSpec(" +
                    "renewalDays=" + renewalDays +
                    ", chain=" + chain +
                    ')';
        }
    }

    static class CertAndKey {
        private final X509Certificate cert;
        private final byte[] key;

        public CertAndKey(X509Certificate cert, byte[] key) {
            this.cert = cert;
            this.key = key;
        }
    }

    private static final ZoneId UTC = ZoneId.of("UTC");

    // TODO we need to increment the version
    // to do that we probably need make fromCrd non-static, load the secret to find the existing version
    // and increment it.
    static final String VERSION = "1";

    public static InternalIssuerSpec clusterCaFromCrd(String clusterName, CertificateAuthority ca) {
        return fromCrd(KafkaResources.clusterCaKeySecretName(clusterName),
                KafkaResources.clusterCaCertificateSecretName(clusterName),
                new Subject.Builder()
                        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
                        // key generation in the DN so the certificates appear distinct during CA key replacement.
                        .withCommonName("cluster-ca v" + VERSION)
                        .withOrganizationName("io.strimzi").build(),
                ca);
    }

    public static InternalIssuerSpec clientsCaFromCrd(String clusterName, CertificateAuthority ca) {
        return fromCrd(KafkaResources.clientsCaKeySecretName(clusterName),
                KafkaResources.clientsCaCertificateSecretName(clusterName),
                new Subject.Builder()
                        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
                        // key generation in the DN so the certificates appear distinct during CA key replacement.
                        .withCommonName("clients-ca v" + VERSION)
                        .withOrganizationName("io.strimzi").build(),
                ca);
    }

    private static InternalIssuerSpec fromCrd(
            String keySecretName, String certSecretName,
            Subject subject,
            CertificateAuthority ca) {
        return new InternalIssuerSpec(
                ca.getRenewalDays(),
                List.of(
                        new CaCertificateReference(
                                certSecretName, "ca.crt",
                                keySecretName, "ca.key",
                                ca.isGenerateCertificateAuthority() ?
                                        new IssuanceParams(subject, ca.getValidityDays(), ca.getCertificateExpirationPolicy()) : null)));
    }

    private final Clock clock;
    private final NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secrets;
    private final CertManager certManager;

    public Chain(Clock clock,
                 NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secrets,
                 CertManager certManager) {
        this.clock = clock;
        this.secrets = secrets;
        this.certManager = certManager;
    }

    public CertificateIssuer reconcile(InternalIssuerSpec chain, OwnerReference ownerRef) throws CertificateException {
        Validator validator = new Validator(clock, chain.getRenewalDays());
        List<X509Certificate> partialCertChain = new ArrayList<>();
        CertAndKey certificate = null;
        byte[] prevKey = null;
        X509Certificate prevCert = null;
        boolean anyIssued = false;
        // For each cert starting from the root:
        for (CaCertificateReference certReference : chain.getChain()) {
            int pathLen = chain.getChain().size() - partialCertChain.size() - 1;
            //  Get the cert
            certificate = loadCertAndKey(
                    certReference.getCertificateSecret(),
                    certReference.getCertificateItem(),
                    certReference.getKeySecret(),
                    certReference.getKeySecretItem());
            boolean issued = false;
            if (certificate == null || certificate.cert == null || anyIssued) {
                if (certReference.isIssuable()) {
                    // If it doesn't exist and is issuable then issue it using the given parameters
                    certificate = issue(certReference.getIssuanceParams(), prevCert, prevKey, certificate.key, pathLen);
                    anyIssued = issued = true;
                } else {
                    // If it does not exist and is not issuable, then throw/report error
                    throw new PkiException(String.format("CA Certificate %s in secret %s does not exist and is not configured to allow automatic issuance",
                            certReference.certificateItem, certReference.certificateSecret));
                }
            } else if (certificate.key == null) {
                throw new PkiException(String.format("CA Key %s in secret %s does not exist and is not configured to allow automatic issuance",
                        certReference.keySecretItem, certReference.keySecret));
            }
            // Validate it using the partial chain built so far
            var x = new ArrayList<>(partialCertChain);
            x.add(certificate.cert);
            Validator.Result validate = validate(validator, x, certificate.cert);
            switch (validate.type()) {
                case OK:
                    break;
                case WILL_EXPIRE:
                case EXPIRED:
                    // reissue if possible
                    certificate = issue(certReference.getIssuanceParams(), prevCert, prevKey, certificate.key, pathLen);
                    anyIssued = issued = true;
                    // validate the issued cert
                    var y = new ArrayList<>(partialCertChain);
                    y.add(certificate.cert);
                    Validator.Result.Type type = validate(validator, y, certificate.cert).type();
                    if (type != Validator.Result.Type.OK) {
                        throw new IllegalStateException("Reissued certificate is not valid: " + type);
                    }

                    break;
                default:
                    throw new IllegalStateException("Unexpected validation result " + validate);
            }
            // store this cert (and the key!)
            if (issued) { //we can't do this conditionally, if we need to reconcile the owner ref
                storeCertAndKey(
                        certReference.getCertificateSecret(),
                        certReference.getCertificateItem(),
                        certReference.getKeySecret(),
                        certReference.getKeySecretItem(),
                        certificate, ownerRef);
            }
            partialCertChain.add(certificate.cert);
            prevCert = certificate.cert;
            prevKey = certificate.key;
        }

        return new InternalCertificateIssuer(clock, certManager, partialCertChain, certificate.key, certificate.cert);
    }

    private void storeCertAndKey(String certificateSecret, String certificateId,
                                 String keySecret, String keyId,
                                 CertAndKey certificate, OwnerReference ownerRef) throws CertificateEncodingException {
        // TODO ownerReference
        // Is it right that we update (potentially) the same secret multiple times (for each cert)
        // TODO How to certs get removed?
        if (certificateSecret.equals(keySecret)) {
            secrets.withName(certificateSecret).createOrReplace(getBuilder(certificateSecret)
                    .editOrNewMetadata()
                        .withOwnerReferences(ownerRef == null ? List.of() : List.of(ownerRef))
                        .withName(certificateSecret)
                    .endMetadata()
                    .addToData(certificateId, Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(certificate.cert)))
                    .addToData(keyId, Base64.getEncoder().encodeToString(certificate.key))
                    .build());
        } else {
            secrets.withName(certificateSecret).createOrReplace(getBuilder(certificateSecret)
                    .editOrNewMetadata()
                        .withOwnerReferences(ownerRef == null ? List.of() : List.of(ownerRef))
                        .withName(certificateSecret)
                    .endMetadata()
                    .addToData(certificateId, Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(certificate.cert)))
                    .build());
            secrets.withName(keySecret).createOrReplace(getBuilder(keySecret)
                    .editOrNewMetadata()
                        .withOwnerReferences(ownerRef == null ? List.of() : List.of(ownerRef))
                        .withName(keySecret)
                    .endMetadata()
                    .addToData(keyId, Base64.getEncoder().encodeToString(certificate.key))
                    .build());
        }
    }

    private SecretBuilder getBuilder(String certificateSecret) {
        Secret s = secrets.withName(certificateSecret).get();
        return s == null ? new SecretBuilder() : new SecretBuilder(s);
    }

    private Validator.Result validate(Validator validator, List<X509Certificate> partialChain, X509Certificate test) {
        var x = new ArrayList<>(partialChain);
        Collections.reverse(x);
        return validator.validate(x, Set.of(new TrustAnchor(partialChain.get(0), null)));
    }

    private CertAndKey loadCertAndKey(String certificateSecret, String certificateId, String keySecret, String keyId) throws CertificateException {
        Secret secret = secrets.withName(certificateSecret).get();
        X509Certificate cert;
        if (secret == null || secret.getData() == null) {
            cert = null;
        } else {
            String src = secret.getData().get(certificateId);
            if (src == null) {
                cert = null;
            } else {
                cert = PkiUtils.x509Certificate(Base64.getDecoder().decode(src));
            }
        }
        if (!certificateSecret.equals(keySecret)) {
            secret = secrets.withName(keySecret).get();
        }
        byte[] key;
        if (secret == null || secret.getData() == null) {
            key = null;
        } else {
            String src = secret.getData().get(keyId);
            if (src == null) {
                key = null;
            } else {
                key = Base64.getDecoder().decode(src);
            }
        }
        return new CertAndKey(cert, key);
    }

    private CertAndKey issue(IssuanceParams issuanceParams, X509Certificate issuerCert, byte[] issuerKey, byte[] existingSubjectKey, int pathLen) {
        Path issuerKeyPath = null;
        Path issuerCertPath = null;
        Path subjectKeyPath = null;
        Path subjectCertPath = null;
        byte[] key;
        X509Certificate cert;
        try {
            try {
                subjectKeyPath = Files.createTempFile(null, null);
                if (issuanceParams.policy == CertificateExpirationPolicy.RENEW_CERTIFICATE && existingSubjectKey != null) {
                    Files.write(subjectKeyPath, existingSubjectKey);
                }
                subjectCertPath = Files.createTempFile(null, null);
                Instant now = clock.instant();
                ZonedDateTime notBefore = now.atZone(UTC);
                ZonedDateTime notAfter = now.plus(issuanceParams.getValidity(), ChronoUnit.DAYS).atZone(UTC);

                if (issuerCert == null) {
                    ((OpenSslCertManager) certManager).generateRootCaCert(
                            issuanceParams.getSubject(), subjectKeyPath.toFile(), subjectCertPath.toFile(),
                            notBefore, notAfter, pathLen);
                } else {
                    issuerKeyPath = Files.createTempFile(null, null);
                    issuerCertPath = Files.createTempFile(null, null);
                    PkiUtils.writePem(issuerCertPath, issuerCert);
                    Files.write(issuerKeyPath, issuerKey);
                    subjectKeyPath = Files.createTempFile(null, null);
                    subjectCertPath = Files.createTempFile(null, null);
                    ((OpenSslCertManager) certManager).generateIntermediateCaCert(
                            issuerKeyPath.toFile(), issuerCertPath.toFile(),
                            issuanceParams.getSubject(),
                            subjectKeyPath.toFile(), subjectCertPath.toFile(),
                            notBefore, notAfter, pathLen);
                }

                cert = PkiUtils.x509Certificate(subjectCertPath);
                key = Files.readAllBytes(subjectKeyPath);
            } catch (IOException | CertificateException e) {
                throw new CaCreationFailed(e);
            }
        } finally {
            delete(issuerKeyPath);
            delete(subjectKeyPath);
            delete(issuerCertPath);
            delete(subjectCertPath);
        }
        return new CertAndKey(cert, key);
    }

}
