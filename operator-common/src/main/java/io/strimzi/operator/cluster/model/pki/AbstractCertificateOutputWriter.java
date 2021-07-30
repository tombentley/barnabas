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
    private final String baseEntryName;
    protected final ObjectMapper statusMapper;
    protected final OwnerReference ownerReference;

    public AbstractCertificateOutputWriter(Reconciliation reconciliation,
                                           String baseEntryName,
                                           OwnerReference ownerReference,
                                           ObjectMapper statusMapper) {
        this.reconciliation = reconciliation;
        this.baseEntryName = baseEntryName;
        this.ownerReference = ownerReference;
        this.statusMapper = statusMapper;
    }

    @SuppressWarnings("unchecked")
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
        if (baseEntryName == null) {
            throw new AssertionError();
        }
        return baseEntryName + ".p12";
    }

    protected String passwordKey(Id id) {
        if (baseEntryName == null) {
            throw new AssertionError();
        }
        return baseEntryName + ".password";
    }

    protected String certKey(Id id) {
        if (baseEntryName == null) {
            throw new AssertionError();
        }
        return baseEntryName + ".crt";
    }

    protected String keyKey(Id id) {
        if (baseEntryName == null) {
            throw new AssertionError();
        }
        return baseEntryName + ".key";
    }

    @SuppressWarnings("unchecked")
    @Override
    public CertificateStatus<Id> loadStatus(Id subjectIdentity) {
        NamedResource<Secret> secretResource = statusResource(subjectIdentity);
        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return CertificateStatus.createNew(subjectIdentity);
        } else {
            String src = secret.getData().get(stateKey(subjectIdentity));
            if (src == null) {
                return CertificateStatus.createNew(subjectIdentity);
            } else {
                try {
                    byte[] s = Base64.getDecoder().decode(src);
                    return statusMapper.readValue(s, CertificateStatus.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    protected abstract String stateKey(Id id);

    protected abstract NamedResource<Secret> statusResource(Id id);
    protected abstract NamedResource<Secret> issuedResource(Id id);

    @Override
    public void storeStatus(CertificateStatus<Id> certificateStatus) {
        Id subjectIdentity = certificateStatus.subjectIdentity();
        NamedResource<Secret> secretResource = statusResource(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        builder.editOrNewMetadata().withOwnerReferences(this.ownerReference == null ? List.of() : List.of(ownerReference)).endMetadata();
        try {
            builder.addToData(stateKey(subjectIdentity), Base64.getEncoder().encodeToString(statusMapper.writeValueAsBytes(certificateStatus)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        LOGGER.debugCr(reconciliation, "Storing certificate: identity: {}, state: {}, secret: {}",
                certificateStatus.subjectIdentity(), certificateStatus.state(), secretResource);
        secretResource.createOrReplace(builder.build());
    }
}
