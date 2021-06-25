/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.util.Base64;

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
    private String zkName;
    private final Resource<Secret> zkSecretResource;
    private String brokersName;
    protected final Resource<Secret> brokerSecretResource;
    private String eoName;
    protected final Resource<Secret> eeSecretResource;
    private String coName;
    protected final Resource<Secret> coSecretResource;
    private String keName;
    protected final Resource<Secret> keSecretResource;
    private String ccName;
    protected final Resource<Secret> ccSecretResource;

    public BrokersCertificateStatusStore(ObjectMapper mapper,
                                         String zkName, Resource<Secret> zkSecretResource,
                                         String brokersName, Resource<Secret> brokerSecretResource,
                                         String eoName, Resource<Secret> eeSecretResource,
                                         String coName, Resource<Secret> coSecretResource,
                                         String keName, Resource<Secret> keSecretResource,
                                         String ccName, Resource<Secret> ccSecretResource) {
        this.mapper = mapper;
        this.zkName = zkName;
        this.zkSecretResource = zkSecretResource;
        this.brokersName = brokersName;
        this.brokerSecretResource = brokerSecretResource;
        this.eoName = eoName;
        this.eeSecretResource = eeSecretResource;
        this.coName = coName;
        this.coSecretResource = coSecretResource;
        this.keName = keName;
        this.keSecretResource = keSecretResource;
        this.ccName = ccName;
        this.ccSecretResource = ccSecretResource;
    }

    @Override
    public CertificateStatus loadStatus(SubjectIdentity subjectIdentity) {
        Resource<Secret> secretResource = secretResource(subjectIdentity);
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
                    return mapper.readValue(s, CertificateStatus.class);
                } catch (IOException e) {
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

    String secretName(SubjectIdentity id) {
        if (id instanceof BrokerIdentity) {
            return brokersName;
        } else if (id instanceof ZooKeeperNodeIdentity) {
            return zkName;
        } else if (id instanceof EntityOperatorIdentity) {
            return eoName;
        } else if (id instanceof ClusterOperatorIdentity) {
            return coName;
        } else if (id instanceof KafkaExporterIdentity) {
            return keName;
        } else if (id instanceof CruiseControlIdentity) {
            return ccName;
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
        String secretName = secretName(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder().withNewMetadata().withName(secretName).endMetadata();
        try {
            builder.addToData(stateKey(subjectIdentity), Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(c)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        secretResource.createOrReplace(builder.build());
    }
}
