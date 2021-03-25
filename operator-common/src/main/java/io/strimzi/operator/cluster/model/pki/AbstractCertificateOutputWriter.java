/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.PasswordGenerator;

public abstract class AbstractCertificateOutputWriter<Id extends SubjectIdentity> implements CertificateOutputWriter {

    private final Resource<Secret> secretResource;
    private final PasswordGenerator pg = new PasswordGenerator(12);
    private final Class<Id> cls;

    public AbstractCertificateOutputWriter(Class<Id> cls, Resource<Secret> secretResource) {
        this.secretResource = Objects.requireNonNull(secretResource);
        this.cls = Objects.requireNonNull(cls);
    }

    @Override
    public void writeIssued(SubjectIdentity subjectIdentity, IssuedCertificate certificate)
            throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        Objects.requireNonNull(subjectIdentity);
        if (cls.isInstance(subjectIdentity)) {
            Id id = (Id) subjectIdentity;
            Secret secret = secretResource.get();
            SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
            try {
                String p12Key = p12Key(id);
                String passwordKey = passwordKey(id);
                if (p12Key != null && passwordKey != null) {
                    String password = pg.generate();
                    builder.addToData(p12Key, Base64.getEncoder().encodeToString(certificate.toPkcs12(password.toCharArray())));
                    builder.addToData(passwordKey, password);
                } else if (p12Key != null || passwordKey != null) {
                    throw new RuntimeException("p12 requires a password: password and p12 must be either both used, or both not used");
                }

                String certKey = certKey(id);
                if (certKey != null) {
                    builder.addToData(certKey, PkiUtils.asPemCertChain(certificate.completeChain()));
                }
                String keyKey = keyKey(id);
                if (keyKey != null) {
                    builder.addToData(keyKey, PkiUtils.asPemKey(certificate.privateKey()));
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if (secret == null) {
                secretResource.create(builder.build());
            } else {
                secretResource.lockResourceVersion(secret.getMetadata().getResourceVersion()).replace(builder.build());
            }
        }
    }

    protected abstract String p12Key(Id id);

    protected abstract String passwordKey(Id id);

    protected abstract String certKey(Id id);

    protected abstract String keyKey(Id id);
}
