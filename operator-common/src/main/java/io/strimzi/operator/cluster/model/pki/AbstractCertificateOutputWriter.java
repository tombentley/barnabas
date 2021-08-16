/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

public abstract class AbstractCertificateOutputWriter<Id extends SubjectIdentity>
        implements CertificateOutputWriter<Id> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractCertificateOutputWriter.class);

    protected final Reconciliation reconciliation;
    private final PasswordGenerator pg = new PasswordGenerator(12);
    protected final ObjectMapper statusMapper;
    protected final OwnerReference ownerReference;

    public AbstractCertificateOutputWriter(Reconciliation reconciliation,
                                           OwnerReference ownerReference,
                                           ObjectMapper statusMapper) {
        this.reconciliation = reconciliation;
        this.ownerReference = ownerReference;
        this.statusMapper = statusMapper;
    }

    @Override
    public void writeIssued(Id subjectIdentity,
                            IssuedCertificate certificate)
            throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        Objects.requireNonNull(subjectIdentity);
        NamedResource<Secret> issuedResource = issuedResource(subjectIdentity);
        LOGGER.infoCr(reconciliation, "XXX Maybe write {} issued to {}", issuedResource, subjectIdentity);

        Secret existingSecret = issuedResource.get();
        SecretBuilder builder = (existingSecret != null ? new SecretBuilder(existingSecret) : new SecretBuilder())
                .editOrNewMetadata().withOwnerReferences(ownerReference == null ? List.of() : List.of(ownerReference)).endMetadata();
        try {
            String p12Key = p12Key(subjectIdentity);
            String passwordKey = passwordKey(subjectIdentity);
            if (p12Key != null && passwordKey != null) {
                String password = pg.generate();
                builder.addToData(p12Key, Base64.getEncoder().encodeToString(certificate.toPkcs12(password.toCharArray())));
                builder.addToData(passwordKey, Base64.getEncoder().encodeToString(password.getBytes(StandardCharsets.US_ASCII)));
            } else if (p12Key != null || passwordKey != null) {
                throw new RuntimeException("p12 requires a password: password and p12 must be either both used, or both not used");
            }

            String certKey = certKey(subjectIdentity);
            if (certKey != null) {
                builder.removeFromData(pendingKey(subjectIdentity));
                builder.addToData(certKey, Base64.getEncoder().encodeToString(PkiUtils.asPemCertChain(certificate.completeChain()).getBytes(StandardCharsets.US_ASCII)));
            }
            String keyKey = keyKey(subjectIdentity);
            if (keyKey != null) {
                builder.addToData(keyKey, Base64.getEncoder().encodeToString(PkiUtils.asPemKey(certificate.privateKey()).getBytes(StandardCharsets.US_ASCII)));
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        Secret newSecret = builder.build();
        if (existingSecret == null) {
            LOGGER.debugCr(reconciliation, "Storing certificate {} issued to {} in new secret {}",
                    certificate.issuedFingerprint(),
                    subjectIdentity,
                    issuedResource);
            issuedResource.create(newSecret);
        } else {
            LOGGER.debugCr(reconciliation, "Storing certificate {} issued to {} in existing secret {}",
                    certificate.issuedFingerprint(),
                    subjectIdentity,
                    issuedResource);
            issuedResource
                    .lockResourceVersion(existingSecret.getMetadata().getResourceVersion())
                    .replace(newSecret);
        }

    }

    protected String p12Key(Id id) {
        return keyPrefix(id) + ".p12";
    }

    protected String passwordKey(Id id) {
        return keyPrefix(id) + ".password";
    }

    protected String certKey(Id id) {
        return keyPrefix(id) + ".crt";
    }

    protected String keyKey(Id id) {
        return keyPrefix(id) + ".key";
    }

    @Override
    public CertificateStatus loadStatus(Id subjectIdentity) {
        NamedResource<Secret> secretResource = statusResource(subjectIdentity);
        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return CertificateStatus.createNew();
        } else {
            try {
                Map<String, String> data = secret.getData();
                String src = data.get(requestedKey(subjectIdentity));
                if (src != null) {
                    return new CertificateStatus(CertificateState.REQUESTED, Base64.getDecoder().decode(src), null);
                }
                src = data.get(pendingKey(subjectIdentity));
                if (src != null) {
                    return new CertificateStatus(CertificateState.PENDING, null, statusMapper.readValue(Base64.getDecoder().decode(src), IssuedCertificate.class));
                }
                src = data.get(certKey(subjectIdentity));
                if (src != null) {

                    IssuedCertificate issuedCertificate = new IssuedCertificate(Base64.getDecoder().decode(data.get(keyKey(subjectIdentity))),
                            PkiUtils.x509Certificate(Base64.getDecoder().decode(src)),
                            List.of(),
                            null);
                    return new CertificateStatus(CertificateState.IN_USE, null, issuedCertificate);
                }
                return CertificateStatus.createNew();
            } catch (IOException | CertificateException e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected abstract String keyPrefix(Id id);
    // TODO these are always the same vv
    protected abstract NamedResource<Secret> statusResource(Id id);
    protected abstract NamedResource<Secret> issuedResource(Id id);

    private String pendingKey(Id subjectIdentity) {
        return keyPrefix(subjectIdentity) + ".pending";
    }

    private String requestedKey(Id subjectIdentity) {
        return keyPrefix(subjectIdentity) + ".requested";
    }

    @Override
    public void storeRequested(Id subjectIdentity, byte[] token) {
        NamedResource<Secret> secretResource = statusResource(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        builder.editOrNewMetadata().withOwnerReferences(this.ownerReference == null ? List.of() : List.of(ownerReference)).endMetadata();
        builder.addToData(requestedKey(subjectIdentity), Base64.getEncoder().encodeToString(token));
        builder.removeFromData(pendingKey(subjectIdentity));
        LOGGER.debugCr(reconciliation, "Storing requested certificate: identity: {}, secret: {}",
                subjectIdentity, secretResource);
        secretResource.createOrReplace(builder.build());
    }

    @Override
    public void storePending(Id subjectIdentity, IssuedCertificate issuedCertificate) {
        NamedResource<Secret> secretResource = statusResource(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        builder.editOrNewMetadata().withOwnerReferences(this.ownerReference == null ? List.of() : List.of(ownerReference)).endMetadata();
        try {
            builder.addToData(pendingKey(subjectIdentity), Base64.getEncoder().encodeToString(statusMapper.writeValueAsBytes(issuedCertificate)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        builder.removeFromData(requestedKey(subjectIdentity));
        LOGGER.debugCr(reconciliation, "Storing requested certificate: identity: {}, secret: {}",
                subjectIdentity, secretResource);
        secretResource.createOrReplace(builder.build());
    }
}
