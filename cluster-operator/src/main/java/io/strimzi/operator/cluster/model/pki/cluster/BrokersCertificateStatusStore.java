/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.CertificateStatusStore;
import io.strimzi.operator.cluster.model.pki.SubjectIdentity;

/**
 * Stores CertificateStatuses in Kube Secret with the following structure:
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: ...
 * data:
 *   # Entries for the current secret
 *   broker-$NUM.state: ...    # The complete state, as JSON
 * </code></pre>
 */
public class BrokersCertificateStatusStore implements CertificateStatusStore {

    protected final ObjectMapper mapper;
    private final Resource<Secret> zkSecretResource;
    protected final Resource<Secret> brokerSecretResource;
    protected final Resource<Secret> eeSecretResource;
    protected final Resource<Secret> coSecretResource;
    protected final Resource<Secret> keSecretResource;
    protected final Resource<Secret> ccSecretResource;

    public BrokersCertificateStatusStore(ObjectMapper mapper,
                                         Resource<Secret> zkSecretResource,
                                         Resource<Secret> brokerSecretResource,
                                         Resource<Secret> eeSecretResource,
                                         Resource<Secret> coSecretResource,
                                         Resource<Secret> keSecretResource,
                                         Resource<Secret> ccSecretResource) {
        this.mapper = mapper;
        this.zkSecretResource = zkSecretResource;
        this.brokerSecretResource = brokerSecretResource;
        this.eeSecretResource = eeSecretResource;
        this.coSecretResource = coSecretResource;
        this.keSecretResource = keSecretResource;
        this.ccSecretResource = ccSecretResource;
    }

    @Override
    public CertificateStatus loadStatus(SubjectIdentity subjectIdentity) {
        Resource<Secret> secretResource = secretResource(subjectIdentity);
        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return CertificateStatus.createNew(subjectIdentity);
        } else {
            String s = secret.getData().get(stateKey(subjectIdentity));
            if (s == null) {
                return CertificateStatus.createNew(subjectIdentity);
            } else {
                try {
                    return mapper.readValue(s, CertificateStatus.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    String stateKey(SubjectIdentity id) {
        if (id instanceof BrokerIdentity) {
            return "broker-" + ((BrokerIdentity) id).brokerId() + ".state";
        } else if (id instanceof ZooKeeperNodeIdentity) {
            return "node-" + ((ZooKeeperNodeIdentity) id).nodeId() + ".state";
        } else if (id instanceof EntityOperatorIdentity) {
            return "entity-operator.state";
        } else if (id instanceof ClusterOperatorIdentity) {
            return "cluster-operator.state";
        } else if (id instanceof KafkaExporterIdentity) {
            return "kafka-exporter.state";
        } else if (id instanceof CruiseControlIdentity) {
            return "cruise-control.state";
        } else {
            throw new IllegalStateException("Unknown identity " + id);
        }
    }

    Resource<Secret> secretResource(SubjectIdentity id) {
        if (id instanceof BrokerIdentity) {
            return brokerSecretResource;
        } else if (id instanceof ZooKeeperNodeIdentity) {
            return zkSecretResource;
        } else if (id instanceof EntityOperatorIdentity) {
            return eeSecretResource;
        } else if (id instanceof ClusterOperatorIdentity) {
            return coSecretResource;
        } else if (id instanceof KafkaExporterIdentity) {
            return keSecretResource;
        } else if (id instanceof CruiseControlIdentity) {
            return ccSecretResource;
        } else {
            throw new IllegalStateException("Unknown identity " + id);
        }
    }


    @Override
    public void storeStatus(CertificateStatus c) {
        SubjectIdentity subjectIdentity = c.subjectIdentity();
        Resource<Secret> secretResource = secretResource(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        try {
            builder.addToData(stateKey(subjectIdentity), mapper.writeValueAsString(c));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        secretResource.createOrReplace(builder.build());
    }
}
