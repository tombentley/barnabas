/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.CertificateController;
import io.strimzi.operator.cluster.model.pki.CertificateIssuer;
import io.strimzi.operator.cluster.model.pki.CertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.CertificateStatusStore;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.LegacyTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustPrivateState;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.operator.cluster.model.pki.Validator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import static io.strimzi.operator.cluster.model.EntityOperator.entityOperatorName;

/**
 * Controls the internal cluster PKI within the CO.
 * Basic flow:
 * <dl>
 *     <dt>{@link #bootstrapCa(CertificateAuthority, OwnerReference)}</dt>
 *     <dd>This will create the `cluster-ca` Secret and populate it with the CA details.</dd>
 *     
 *     <dt>{@code #issue*Certificates()}</dt>
 *     <dd>this will reconcile the CertificateController,
 *         possibly creating a bunch of pending certificates.
 *         It will figure out the unused cert too, somehow.
 *         It should return something which we can feed to the KafkaRoller predicate
 *         And also ZKRoller etc. </dd>
 *
 *     <dt>Some rolling happens</dt>
 *     <dd>if needed to establish trust.</dd>
 *
 *     <dt>{@link #deployCertificates()}</dt>
 *     <dd>This will deploy the pending certificates, copying them into the Secrets where they can be used.</dd>
 *
 *     <dt>Some rolling happens</dt>
 *     <dd>or whatever action is needed for the certs to actually be used.</dd>
 * </dl>
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class ClusterPki {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(ZooKeeperNodeIdentity.class, "zookeeper"),
                new NamedType(BrokerIdentity.class, "kafka"),
                new NamedType(EntityOperatorIdentity.class, "entity-operator"),
                new NamedType(ClusterOperatorIdentity.class, "cluster-operator"),
                new NamedType(KafkaExporterIdentity.class, "kafka-exporter"));
    }

    private final Chain chain;
    private final Reconciliation reconciliation;
    private CertificateController controller;
    private Set<TrustRoot> newTrustRoots;
    private Set<TrustRoot> usedTrustRoots;

    private static CertificateOutputWriter co(Resource<Secret> secretResource) {
        return new AbstractCertificateOutputWriter<>(ClusterOperatorIdentity.class, secretResource, "cluster-operator") { };
    }

    private static CertificateOutputWriter cc(Resource<Secret> secretResource) {
        return new AbstractCertificateOutputWriter<>(CruiseControlIdentity.class, secretResource, "cluster-operator") { };
    }

    private static CertificateOutputWriter eo(Resource<Secret> secretResource) {
        return new AbstractCertificateOutputWriter<>(EntityOperatorIdentity.class, secretResource, "entity-operator") { };
    }

    private static CertificateOutputWriter ke(Resource<Secret> secretResource) {
        return new AbstractCertificateOutputWriter<>(KafkaExporterIdentity.class, secretResource, "kafka-exporter") { };
    }

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ClusterPki.class.getName());

    private final Clock clock = Clock.systemUTC();
    private final String namespace;
    private final String cluster;
    private final List<CertificateOutputWriter> certOutputWriters;

    private CertificateIssuer issuer;
    private final CertificateStatusStore certificateStatusStore;
    private final TrustPrivateState trustPrivateState;
    private TrustController trustController;

    public ClusterPki(Reconciliation reconciliation,
                      KubernetesClient client,
                      CertManager certManager,
                      String namespace, String cluster) {
        this.reconciliation = Objects.requireNonNull(reconciliation);
        this.namespace = Objects.requireNonNull(namespace);
        this.cluster = Objects.requireNonNull(cluster);

        this.chain = new Chain(clock,
                client.secrets().inNamespace(namespace),
                certManager);

        this.certificateStatusStore = new BrokersCertificateStatusStore(MAPPER,
                ZookeeperCluster.nodesSecretName(cluster), client.secrets().inNamespace(namespace).withName(ZookeeperCluster.nodesSecretName(cluster)),
                KafkaCluster.brokersSecretName(cluster), client.secrets().inNamespace(namespace).withName(KafkaCluster.brokersSecretName(cluster)),
                EntityOperator.secretName(cluster), client.secrets().inNamespace(namespace).withName(EntityOperator.secretName(cluster)),
                ClusterOperator.secretName(cluster), client.secrets().inNamespace(namespace).withName(ClusterOperator.secretName(cluster)),
                KafkaExporter.secretName(cluster), client.secrets().inNamespace(namespace).withName(KafkaExporter.secretName(cluster)),
                CruiseControl.secretName(cluster), client.secrets().inNamespace(namespace).withName(CruiseControl.secretName(cluster))
                );
        this.certOutputWriters = List.of(
                new ZookeeperCertificateOutputWriter(client.secrets().inNamespace(namespace)
                        .withName(ZookeeperCluster.nodesSecretName(cluster))),
                new NewBrokersCertificateOutputWriter(client.secrets().inNamespace(namespace)
                        .withName(KafkaCluster.brokersSecretName(cluster))),
                eo(client.secrets().inNamespace(namespace).withName(KafkaResources.entityOperatorSecretName(cluster))),
                co(client.secrets().inNamespace(namespace).withName(ClusterOperator.secretName(cluster))),
                cc(client.secrets().inNamespace(namespace).withName(CruiseControl.secretName(cluster))),
                ke(client.secrets().inNamespace(namespace).withName(KafkaExporterResources.secretName(cluster)))
        );
        this.trustPrivateState = new TrustPrivateState(cluster + "-cluster-pki-state", client.secrets().inNamespace(namespace).withName(cluster + "-cluster-pki-state")); // TODO fix the name

        this.trustController = new TrustController(trustPrivateState,
                List.of(new LegacyTrustOutputWriter(client.secrets().inNamespace(namespace)
                        .withName(ZookeeperCluster.nodesSecretName(cluster)))));
    }

    /**
     * Create the CA according to the certificateAuthority
     * @param clusterCa The Kafka CR CA.
     * @param ownerReference The owner reference
     * @throws CertificateException if problems.
     */
    public void bootstrapCa(CertificateAuthority clusterCa, OwnerReference ownerReference) throws CertificateException {
        if (clusterCa == null) {
            clusterCa = new CertificateAuthority();
            clusterCa.setValidityDays(365);
            clusterCa.setRenewalDays(30);
        }
        long t0 = System.nanoTime();
        issuer = chain.reconcile(reconciliation, Chain.clusterCaFromCrd(cluster, clusterCa), clusterCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
        controller = controller(clusterCa);
        newTrustRoots = new HashSet<>();
        usedTrustRoots = new HashSet<>();
    }


    public void issueZkCertificates(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        long t0 = System.nanoTime();
        // possibly create a bunch of pending certificates
        int zkReplicas = kafka.getSpec().getZookeeper().getReplicas() + 1;
        for (int nodeId = 1; nodeId < zkReplicas; nodeId++) {
            collect(controller.reconcile(zkSubject(namespace, cluster, nodeId)));
        }
        LOGGER.debugCr(reconciliation, "ZK certificate issuance took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
    }

    public void issueKafkaCertificates(Kafka kafka,
                                    Set<String> externalBootstrapAddresses,
                                    Map<Integer, Set<String>> externalAddresses) throws CertificateException {
        Objects.requireNonNull(this.controller);
        long t0 = System.nanoTime();
        int kafkaReplicas = kafka.getSpec().getKafka().getReplicas();
        for (int brokerId = 0; brokerId < kafkaReplicas; brokerId++) {
            collect(controller.reconcile(
                    brokerSubject(namespace, cluster, brokerId,
                            externalBootstrapAddresses, externalAddresses)));
        }
        LOGGER.debugCr(reconciliation, "Kafka certificate issuance took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
    }

    public void issueEoCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        io.strimzi.api.kafka.model.EntityOperatorSpec entityOperator = kafka.getSpec().getEntityOperator();
        if (entityOperator != null) {
            long t0 = System.nanoTime();
            collect(controller.reconcile(new RequiredCertificate(new EntityOperatorIdentity(),
                    new Subject.Builder()
                            .withCommonName(entityOperatorName(cluster))
                            .withOrganizationName(Ca.IO_STRIMZI)
                            .build())));
            LOGGER.debugCr(reconciliation, "EO certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
        }
    }

    public void issueKafkaExporterCertificates(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        io.strimzi.api.kafka.model.KafkaExporterSpec i = kafka.getSpec().getKafkaExporter();
        if (i != null) {
            long t0 = System.nanoTime();
            collect(controller.reconcile(new RequiredCertificate(new KafkaExporterIdentity(),
                    new Subject.Builder()
                            .withCommonName(KafkaExporterResources.deploymentName(cluster))
                            .withOrganizationName(Ca.IO_STRIMZI)
                            .build())));
            LOGGER.debugCr(reconciliation, "Kafka Exporter certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
        }
    }

    public void issueCcCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        if (kafka.getSpec().getCruiseControl() != null) {
            // TODO collect all the certs which we're waiting for trust on
            long t0 = System.nanoTime();
            collect(controller.reconcile(new RequiredCertificate(new CruiseControlIdentity(),
                    new Subject.Builder()
                            .withCommonName(CruiseControlResources.deploymentName(cluster))
                            .withOrganizationName(Ca.IO_STRIMZI)
                            .build())));
            LOGGER.debugCr(reconciliation, "Cruise Control certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
        }
    }

    public void issueCoCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        long t0 = System.nanoTime();
        collect(controller.reconcile(new RequiredCertificate(new ClusterOperatorIdentity(),
                new Subject.Builder()
                        .withCommonName("cluster-operator")
                        .withOrganizationName(Ca.IO_STRIMZI)
                        .build())));
        LOGGER.debugCr(reconciliation, "Cluster Operator certificate issuance took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
        // TODO It will figure out the unused certs too, somehow.
        // It should return something which we can feed to the KafkaRoller predicate
        // And also ZK etc.
    }

    private void collect(CertificateReconcileResult result) {
        LOGGER.debugCr(reconciliation, "Certificate reconciliation result {}", result);
        if (result instanceof CertificateReconcileResult.WaitingForIssuance) {
        } else if (result instanceof CertificateReconcileResult.IssuedBad) {
        } else if (result instanceof CertificateReconcileResult.PendingBad) {
        } else if (result instanceof CertificateReconcileResult.WaitingForTrust) {
            newTrustRoots.add(((CertificateReconcileResult.WaitingForTrust) result).trustRoot());
            usedTrustRoots.add(((CertificateReconcileResult.WaitingForTrust) result).trustRoot());
        } else if (result instanceof CertificateReconcileResult.OkIssued) {
            usedTrustRoots.add(((CertificateReconcileResult.OkIssued) result).trustRoot());
        } else if (result instanceof CertificateReconcileResult.OkNoop) {
            usedTrustRoots.add(((CertificateReconcileResult.OkNoop) result).trustRoot());
        } else {
            throw new IllegalStateException("Unknown subclass " + result);
        }
    }

    private CertificateController controller(CertificateAuthority clusterCa) {
        Validator validator = new Validator(clock, clusterCa.getRenewalDays());
        return new CertificateController(trustPrivateState, certificateStatusStore,
                certOutputWriters, issuer,
                validator, clusterCa.getValidityDays());
    }

    static RequiredCertificate brokerSubject(String namespace,
                                            String cluster,
                                            int brokerId,
                                            Set<String> externalBootstrapAddresses,
                                            Map<Integer, Set<String>> externalAddresses) {
        DnsNameGenerator kafkaDnsGenerator = DnsNameGenerator.of(namespace, KafkaCluster.serviceName(cluster));
        DnsNameGenerator kafkaHeadlessDnsGenerator = DnsNameGenerator.of(namespace, KafkaCluster.headlessServiceName(cluster));

        Subject.Builder subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(KafkaCluster.kafkaClusterName(cluster));

        subject.addDnsName(KafkaCluster.serviceName(cluster));
        subject.addDnsName(String.format("%s.%s", KafkaCluster.serviceName(cluster), namespace));
        subject.addDnsName(kafkaDnsGenerator.serviceDnsNameWithoutClusterDomain());
        subject.addDnsName(kafkaDnsGenerator.serviceDnsName());
        subject.addDnsName(KafkaCluster.headlessServiceName(cluster));
        subject.addDnsName(String.format("%s.%s", KafkaCluster.headlessServiceName(cluster), namespace));
        subject.addDnsName(kafkaHeadlessDnsGenerator.serviceDnsNameWithoutClusterDomain());
        subject.addDnsName(kafkaHeadlessDnsGenerator.serviceDnsName());
        subject.addDnsName(KafkaCluster.podDnsName(namespace, cluster, brokerId));
        subject.addDnsName(KafkaCluster.podDnsNameWithoutClusterDomain(namespace, cluster, brokerId));

        if (externalBootstrapAddresses != null)   {
            for (String dnsName : externalBootstrapAddresses) {
                if (!subject.isValidIpv4Address(dnsName)) {
                    subject.addDnsName(dnsName);
                } else {
                    subject.addIpAddress(dnsName);
                }
            }
        }

        if (externalAddresses != null && externalAddresses.get(brokerId) != null)   {
            for (String dnsName : externalAddresses.get(brokerId)) {
                if (!subject.isValidIpv4Address(dnsName)) {
                    subject.addDnsName(dnsName);
                } else {
                    subject.addIpAddress(dnsName);
                }
            }
        }
        return new RequiredCertificate(new BrokerIdentity(brokerId), subject.build());

    }

    static RequiredCertificate zkSubject(String namespace,
                     String cluster,
                     int nodeId) {
        DnsNameGenerator zkDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.serviceName(cluster));
        DnsNameGenerator zkHeadlessDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.headlessServiceName(cluster));

        Subject.Builder subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
        subject.addDnsName(ZookeeperCluster.serviceName(cluster));
        subject.addDnsName(String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
        subject.addDnsName(zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkDnsGenerator.serviceDnsName());
        subject.addDnsName(ZookeeperCluster.podDnsName(namespace, cluster, nodeId));
        subject.addDnsName(ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, nodeId));
        subject.addDnsName(zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkDnsGenerator.wildcardServiceDnsName());
        subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsName());
        return new RequiredCertificate(new ZooKeeperNodeIdentity(nodeId), subject.build());
    }

    // Kafka roller will then run
    public boolean newTrust() {
        return !newTrustRoots.isEmpty();
    }

    public void trust() throws CertificateEncodingException {
        Set<TrustRoot> newState = trustPrivateState.load().values().stream().map(trustRoot -> {
            if (newTrustRoots.contains(trustRoot)) {
                // TODO this is fishy:
                // a) what is enforcing the valid transitions
                // b) why the need for the resourceVersion argument, then the TrustRoot already knows its rv?
                return trustRoot.toTrusted(trustRoot.resourceVersion());
            } else {
                return trustRoot.toZombie();
            }
        }).collect(Collectors.toSet());
        // If a new CA cert was used this will
        trustController.reconcile(newState);
    }
    
    public void deployCertificates() {
        // This will deploy the pending certificates 
    }
}
