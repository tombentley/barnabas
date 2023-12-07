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
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

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
                throw new IllegalArgumentException("");
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

        public CaCertificateReference(String certificateSecret, String certificateItem,
                                      String keySecret, String keySecretItem,
                                      IssuanceParams issuanceParams) {
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
        private final boolean reissue;

        public CertAndKey(X509Certificate cert, byte[] key, boolean reissue) {
            this.cert = cert;
            this.key = key;
            this.reissue = reissue;
        }
    }

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(Chain.class);

    private static final ZoneId UTC = ZoneId.of("UTC");

    // TODO we need to increment the version
    // to do that we probably need make fromCrd non-static, load the secret to find the existing version
    // and increment it.
    static int clusterVersion = 0;
    static int clientsVersion = 0;

    public static InternalIssuerSpec clusterCaFromCrd(String clusterName, CertificateAuthority ca) {
        clusterVersion++;
        return fromCrd(KafkaResources.clusterCaKeySecretName(clusterName),
                KafkaResources.clusterCaCertificateSecretName(clusterName),
                new Subject.Builder()
                        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
                        // key generation in the DN so the certificates appear distinct during CA key replacement.
                        .withCommonName("cluster-ca v" + clusterVersion)
                        .withOrganizationName("io.strimzi").build(),
                ca);
    }

    public static InternalIssuerSpec clientsCaFromCrd(String clusterName, CertificateAuthority ca) {
        clientsVersion++;
        return fromCrd(KafkaResources.clientsCaKeySecretName(clusterName),
                KafkaResources.clientsCaCertificateSecretName(clusterName),
                new Subject.Builder()
                        // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
                        // key generation in the DN so the certificates appear distinct during CA key replacement.
                        .withCommonName("clients-ca v" + clientsVersion)
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

    private final Reconciliation reconciliation;
    private final Clock clock;
    private final NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secrets;
    private final CertManager certManager;
    private final TrustManager trustManager;

    public Chain(Reconciliation reconciliation,
                 Clock clock,
                 NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secrets,
                 CertManager certManager,
                 TrustManager trustManager) {
        this.reconciliation = reconciliation;
        this.clock = clock;
        this.secrets = secrets;
        this.certManager = certManager;
        this.trustManager = trustManager;
    }

    public CertificateIssuer reconcile(InternalIssuerSpec chain, OwnerReference ownerRef) throws CertificateException {
        Validator validator = new Validator(clock, chain.getRenewalDays());
        List<X509Certificate> partialCertChain = new ArrayList<>();
        CertAndKey originalCertificate = null;
        byte[] prevKey = null;
        X509Certificate prevCert = null;
        boolean anyIssued = false;
        // For each cert starting from the root:
        for (CaCertificateReference certReference : chain.getChain()) {
            CertAndKey newCertificate = null;
            int pathLen = chain.getChain().size() - partialCertChain.size() - 1;
            //  Get the cert
            originalCertificate = loadCertAndKey(
                    certReference.getCertificateSecret(),
                    certReference.getCertificateItem(),
                    certReference.getKeySecret(),
                    certReference.getKeySecretItem());
            anyIssued |= originalCertificate.reissue;
            if (anyIssued) {
                if (certReference.isIssuable()) {
                    LOGGER.infoCr(reconciliation, "{} does not exist; creating", certReference);
                    newCertificate = issue(certReference.getIssuanceParams(), prevCert, prevKey, originalCertificate.key, pathLen);
                    // validate the issued cert
                    Validator.Result.Type type = validate(validator, validationChain(partialCertChain, newCertificate), newCertificate.cert).type();
                    if (type != Validator.Result.Type.OK) {
                        throw new IllegalStateException("Reissued certificate " + certReference + " is not valid: " + type);
                    }
                } else {
                    throw new PkiException(
                            originalCertificate.cert == null ?
                                    String.format("CA Certificate %s in secret %s does not exist and is not configured to allow automatic issuance",
                                            certReference.certificateItem, certReference.certificateSecret) :
                                    originalCertificate.key == null ?
                                            String.format("CA Key %s in secret %s does not exist and is not configured to allow automatic issuance",
                                                    certReference.keySecretItem, certReference.keySecret) :
                                            "A previous certificate in the path needed to be reissued, but this one could not be.");
                }
            } else {
                // Validate it using the partial chain built so far
                Validator.Result validate = validate(validator, validationChain(partialCertChain, originalCertificate), originalCertificate.cert);
                switch (validate.type()) {
                    case OK:
                        LOGGER.debugCr(reconciliation, "CA certificate {} is valid", certReference);
                        break;
                    case WILL_EXPIRE:
                    case HAS_EXPIRED:
                        LOGGER.infoCr(reconciliation, "{} {}, reissuing", certReference, validate.type());
                        // reissue if possible
                        newCertificate = issue(certReference.getIssuanceParams(), prevCert, prevKey, originalCertificate.key, pathLen);
                        anyIssued = true;
                        // validate the issued cert
                        Validator.Result.Type type = validate(validator, validationChain(partialCertChain, newCertificate), newCertificate.cert).type();
                        if (type != Validator.Result.Type.OK) {
                            throw new IllegalStateException("Reissued certificate " + certReference + " is not valid: " + type);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected validation result " + validate);
                }
            }
            // store this cert and key
            if (anyIssued) { //TODO we can't do this conditionally, if we need to reconcile the owner ref
                storeCertAndKey(
                        certReference.getCertificateSecret(),
                        certReference.getCertificateItem(),
                        certReference.getKeySecret(),
                        certReference.getKeySecretItem(),
                        newCertificate, ownerRef);
                if (originalCertificate.cert != null) {
                    trustManager.phaseOut(Set.of(originalCertificate.cert));
                }
                originalCertificate = newCertificate;
            }
            partialCertChain.add(originalCertificate.cert);
            prevCert = originalCertificate.cert;
            prevKey = originalCertificate.key;
        }

//    }
//
//    public CertificateIssuer instantiate() throws CertificateException {

        return new InternalCertificateIssuer(reconciliation, clock, certManager, partialCertChain, originalCertificate.key, originalCertificate.cert);
    }

    private ArrayList<X509Certificate> validationChain(List<X509Certificate> partialCertChain, CertAndKey certificate) {
        var validationChain = new ArrayList<>(partialCertChain);
        validationChain.add(certificate.cert);
        return validationChain;
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

    private CertAndKey loadCertAndKey(String certSecretName, String certificateId, String keySecretName, String keyId) throws CertificateException {
        Secret certSecret = secrets.withName(certSecretName).get();
        LOGGER.debugCr(reconciliation, "Loading existing CA cert from {} item {}", certSecretName, certificateId);
        final X509Certificate cert;
        boolean reissue = false;
        if (certSecret == null || certSecret.getData() == null) {
            LOGGER.debugCr(reconciliation, "Cert does not exist");
            cert = null;
            reissue = true;
        } else {
            String src = certSecret.getData().get(certificateId);
            if (src == null) {
                LOGGER.debugCr(reconciliation, "Cert does not exist");
                cert = null;
                reissue = true;
            } else {
                cert = PkiUtils.x509Certificate(Base64.getDecoder().decode(src));
                if (Annotations.booleanAnnotation(certSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, false)) {
                    LOGGER.infoCr(reconciliation, "Secret {} annotated with {}: Not loading existing cert",
                            certSecretName, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW);
                    reissue = true;
                } else {
                    LOGGER.debugCr(reconciliation, "Cert exists");
                }
            }
        }

        Secret keySecret;
        if (certSecretName.equals(keySecretName)) {
            keySecret = certSecret;
        } else {
            keySecret = secrets.withName(keySecretName).get();
        }
        LOGGER.debugCr(reconciliation, "Loading existing CA key from {} item {}", keySecretName, keyId);
        final byte[] key;
        if (keySecret == null || keySecret.getData() == null) {
            LOGGER.debugCr(reconciliation, "Key does not exist");
            key = null;
            reissue = true;
        } else {
            String src = keySecret.getData().get(keyId);
            if (src == null) {
                LOGGER.debugCr(reconciliation, "Key does not exist");
                key = null;
                reissue = true;
            } else {
                key = Base64.getDecoder().decode(src);
                if (Annotations.booleanAnnotation(keySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, false)) {
                    LOGGER.debugCr(reconciliation, "Secret {} annotated with {}: Not loading existing key",
                            keySecretName, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE);
                    reissue = true;
                } else {
                    LOGGER.debugCr(reconciliation, "Key existsq");
                }
            }
        }
        return new CertAndKey(cert, key, reissue);
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
                    certManager.generateRootCaCert(
                            issuanceParams.getSubject(), subjectKeyPath.toFile(), subjectCertPath.toFile(),
                            notBefore, notAfter, pathLen);
                } else {
                    issuerKeyPath = Files.createTempFile(null, null);
                    issuerCertPath = Files.createTempFile(null, null);
                    PkiUtils.writePem(issuerCertPath, issuerCert);
                    Files.write(issuerKeyPath, issuerKey);
                    subjectKeyPath = Files.createTempFile(null, null);
                    subjectCertPath = Files.createTempFile(null, null);
                    certManager.generateIntermediateCaCert(
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
        return new CertAndKey(cert, key, false);
    }

}
