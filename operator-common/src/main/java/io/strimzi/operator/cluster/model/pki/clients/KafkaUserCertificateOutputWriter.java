/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
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
}
