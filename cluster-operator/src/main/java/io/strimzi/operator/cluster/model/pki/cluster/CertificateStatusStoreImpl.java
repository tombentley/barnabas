/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.CertificateStatusStore;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.SubjectIdentity;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

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
public class CertificateStatusStoreImpl implements CertificateStatusStore {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertificateStatusStoreImpl.class);

    protected final ObjectMapper mapper;
    private final OwnerReference ownerReference;
    private final NamedResource<Secret> zkSecretResource;
    private final NamedResource<Secret> brokerSecretResource;
    private final NamedResource<Secret> eeSecretResource;
    private final NamedResource<Secret> coSecretResource;
    private final NamedResource<Secret> keSecretResource;
    private final NamedResource<Secret> ccSecretResource;
    private final Reconciliation reconciliation;
    private final String cluster;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public CertificateStatusStoreImpl(Reconciliation reconciliation,
                                      ObjectMapper mapper,
                                      OwnerReference ownerReference,
                                      String cluster,
                                      NamedResource<Secret> zkSecretResource,
                                      NamedResource<Secret> brokerSecretResource,
                                      NamedResource<Secret> eeSecretResource,
                                      NamedResource<Secret> coSecretResource,
                                      NamedResource<Secret> keSecretResource,
                                      NamedResource<Secret> ccSecretResource) {
        this.reconciliation = reconciliation;
        this.mapper = mapper;
        this.ownerReference = ownerReference;
        this.cluster = cluster;
        this.zkSecretResource = zkSecretResource;
        this.brokerSecretResource = brokerSecretResource;
        this.eeSecretResource = eeSecretResource;
        this.coSecretResource = coSecretResource;
        this.keSecretResource = keSecretResource;
        this.ccSecretResource = ccSecretResource;
    }

    @Override
    public CertificateStatus loadStatus(SubjectIdentity subjectIdentity) {
        NamedResource<Secret> secretResource = secretResource(subjectIdentity);
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
            return KafkaCluster.kafkaPodName(cluster, ((BrokerIdentity) id).brokerId()) + ".state";
        } else if (id instanceof ZooKeeperNodeIdentity) {
            return ZookeeperCluster.zookeeperPodName(cluster, ((ZooKeeperNodeIdentity) id).nodeId()) + ".state";
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

    NamedResource<Secret> secretResource(SubjectIdentity id) {
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
        NamedResource<Secret> secretResource = secretResource(subjectIdentity);
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        builder.editOrNewMetadata().withOwnerReferences(this.ownerReference == null ? List.of() : List.of(ownerReference)).endMetadata();
        try {
            builder.addToData(stateKey(subjectIdentity), Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(c)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        LOGGER.debugCr(reconciliation, "Storing certificate: identity: {}, state: {}, secret: {}",
                c.subjectIdentity(), c.state(), secretResource);
        secretResource.createOrReplace(builder.build());
    }
}
