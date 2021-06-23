/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

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
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
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
import io.strimzi.operator.cluster.model.KafkaCluster;
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
 *     <dt>{@link #bootstrapCa(CertificateAuthority)}</dt>
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
    private final KubernetesClient client;
    private final String namespace;
    private final String cluster;
    private final CertManager certManager;
    private final List<CertificateOutputWriter> certOutputWriters;
    private Resource<Secret> caKeySecretResource;

    private CertificateIssuer issuer;
    private final CertificateStatusStore certificateStatusStore;
    private final TrustPrivateState trustPrivateState;
    private TrustController trustController;

    public ClusterPki(Reconciliation reconciliation, KubernetesClient client,
                      CertManager certManager,
                      String namespace, String cluster) {
        this.reconciliation = Objects.requireNonNull(reconciliation);
        this.client = Objects.requireNonNull(client);
        this.certManager = Objects.requireNonNull(certManager);
        this.namespace = Objects.requireNonNull(namespace);
        this.cluster = Objects.requireNonNull(cluster);

        this.chain = new Chain(clock,
                client.secrets().inNamespace(namespace),
                certManager);

        this.caKeySecretResource = client.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaKeySecretName(cluster));
        this.certificateStatusStore = new BrokersCertificateStatusStore(MAPPER,
                client.secrets().inNamespace(namespace).withName(ZookeeperCluster.nodesSecretName(cluster)),
                client.secrets().inNamespace(namespace).withName(KafkaCluster.brokersSecretName(cluster)));
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
        this.trustPrivateState = new TrustPrivateState(client.secrets().inNamespace(namespace).withName("")); // TODO fix the name

        this.trustController = new TrustController(trustPrivateState,
                List.of(new LegacyTrustOutputWriter(client.secrets().inNamespace(namespace)
                        .withName(ZookeeperCluster.nodesSecretName(cluster)))));
    }

    /**
     * Create the CA according to the certificateAuthority
     * @param clusterCa The Kafka CR CA.
     * @throws CertificateException if problems.
     */
    public void bootstrapCa(CertificateAuthority clusterCa) throws CertificateException {
        if (clusterCa == null) {
            clusterCa = new CertificateAuthority();
            clusterCa.setValidityDays(365);
            clusterCa.setRenewalDays(30);
        }
        OwnerReference ownerReference;
        if (clusterCa.isGenerateSecretOwnerReference()) {
            ownerReference = new OwnerReferenceBuilder().build();
        } else {
            ownerReference = null;
        }
        long t0 = System.nanoTime();
        issuer = chain.reconcile(reconciliation, Chain.clusterCaFromCrd(cluster, clusterCa), ownerReference);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms", (System.nanoTime() - t0) / 1_000_000.0);
        controller = controller(clusterCa);
    }


    public void issueZkCertificates(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        long t0 = System.nanoTime();
        // possibly create a bunch of pending certificates
        Set<TrustRoot> neededRoots = new HashSet<>();
        int zkReplicas = kafka.getSpec().getZookeeper().getReplicas() + 1;
        for (int nodeId = 1; nodeId < zkReplicas; nodeId++) {
            CertificateReconcileResult reconcile = controller.reconcile(zkSubject(namespace, cluster, nodeId));
            neededRoots.add(reconcile.trustRoot());
        }
        Set<TrustRoot> newState = trustPrivateState.load().values().stream().map(trustRoot -> {
            if (neededRoots.contains(trustRoot)) {
                return trustRoot;
            } else {
                return trustRoot.toZombie();
            }
        }).collect(Collectors.toSet());
        trustController.reconcile(newState);
        LOGGER.debugCr(reconciliation, "ZK certificate issuance took {}ms", (System.nanoTime() - t0) / 1_000_000.0);
    }

    public void issueKafkaCertificates(Kafka kafka,
                                    Set<String> externalBootstrapAddresses,
                                    Map<Integer, Set<String>> externalAddresses) throws CertificateException {
        Objects.requireNonNull(this.controller);
        int kafkaReplicas = kafka.getSpec().getKafka().getReplicas();
        for (int brokerId = 0; brokerId < kafkaReplicas; brokerId++) {
            controller.reconcile(
                    brokerSubject(namespace, cluster, brokerId, externalBootstrapAddresses, externalAddresses));
        }
    }

    public void issueEoCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        io.strimzi.api.kafka.model.EntityOperatorSpec entityOperator = kafka.getSpec().getEntityOperator();
        if (entityOperator != null) {
            controller.reconcile(new RequiredCertificate(EntityOperatorIdentity.INSTANCE,
                    new Subject.Builder()
                            .withCommonName(entityOperatorName(cluster))
                            .withOrganizationName(Ca.IO_STRIMZI)
                            .build()));
        }
    }

    public void issueKafkaExporterCertificates(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        io.strimzi.api.kafka.model.KafkaExporterSpec i = kafka.getSpec().getKafkaExporter();
        if (i != null) {
            controller.reconcile(new RequiredCertificate(KafkaExporterIdentity.INSTANCE,
                    new Subject.Builder()
                            .withCommonName(KafkaExporterResources.deploymentName(cluster))
                            .withOrganizationName(Ca.IO_STRIMZI)
                            .build()));
        }
    }

    public void issueCcCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        controller.reconcile(new RequiredCertificate(CruiseControlIdentity.INSTANCE,
                new Subject.Builder()
                        .withCommonName(CruiseControlResources.deploymentName(cluster))
                        .withOrganizationName(Ca.IO_STRIMZI)
                        .build()));
    }

    public void issueCoCertificate(Kafka kafka) throws CertificateException {
        Objects.requireNonNull(this.controller);
        controller.reconcile(new RequiredCertificate(ClusterOperatorIdentity.INSTANCE,
                new Subject.Builder()
                        .withCommonName("cluster-operator")
                        .withOrganizationName(Ca.IO_STRIMZI)
                        .build()));

        // TODO It will figure out the unused certs too, somehow.
        // It should return something which we can feed to the KafkaRoller predicate
        // And also ZK etc.
    }

    private CertificateController controller(CertificateAuthority clusterCa) {
        Validator validator = new Validator(clock, clusterCa.getRenewalDays());
        CertificateController controller = new CertificateController(trustPrivateState, certificateStatusStore,
                certOutputWriters, issuer,
                validator, clusterCa.getValidityDays());
        return controller;
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
    
    public void deployCertificates() {
        // This will deploy the pending certificates 
    }
    
    // Kafka roller will then need to run
}
