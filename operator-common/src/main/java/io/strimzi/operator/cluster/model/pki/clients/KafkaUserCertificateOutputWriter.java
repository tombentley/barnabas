/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

public class KafkaUserCertificateOutputWriter extends AbstractCertificateOutputWriter<KafkaUserIdentity> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaUserCertificateOutputWriter.class);
    private final NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secretResource;

    public KafkaUserCertificateOutputWriter(Reconciliation reconciliation,
                                            NonNamespaceOperation<Secret, SecretList, Resource<Secret>> secretResource,
                                            OwnerReference ownerReference,
                                            ObjectMapper mapper) {
        super(reconciliation, "user", ownerReference, mapper);
        this.secretResource = secretResource;
    }

    // TODO a user secret also contains the trusted CA certificate(s)

    @Override
    protected String p12Key(KafkaUserIdentity kafkaUserIdentity) {
        return "user.p12";
    }

    @Override
    protected String passwordKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.password";
    }

    @Override
    protected String certKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.crt";
    }

    @Override
    protected String keyKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.key";
    }


    @Override
    public CertificateStatus loadStatus(KafkaUserIdentity subjectIdentity) {
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

    @Override
    protected String stateKey(KafkaUserIdentity id) {
        return id.username() + ".state";
    }

    @Override
    protected NamedResource<Secret> statusResource(KafkaUserIdentity id) {
        return new NamedResource<>(secretResource, id.username());
    }

    @Override
    protected NamedResource<Secret> issuedResource(KafkaUserIdentity id) {
        return new NamedResource<>(secretResource, id.username());
    }


    @Override
    public void storeStatus(CertificateStatus<KafkaUserIdentity> certificateStatus) {
        KafkaUserIdentity subjectIdentity = certificateStatus.subjectIdentity();
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
