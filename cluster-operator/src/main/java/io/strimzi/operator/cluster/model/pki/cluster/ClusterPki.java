/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
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
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TrustController2;
import io.strimzi.operator.cluster.model.pki.TrustManager;
import io.strimzi.operator.cluster.model.pki.UseManager;
import io.strimzi.operator.cluster.model.pki.Validator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import static io.strimzi.operator.cluster.model.EntityOperator.entityOperatorName;

/**
 * Controls the internal cluster PKI within the CO.
 * Basic flow:
 * <dl>
 *     <dt>{@link #bootstrapCa(Reconciliation, String, String,
 *                                CertificateAuthority,
 *                                OwnerReference)}</dt>
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
 *     <dt>Some rolling happens</dt>
 *     <dd>or whatever action is needed for the certs to actually be used.</dd>
 * </dl>
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class ClusterPki {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ClusterPki.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(EntityOperatorIdentity.class, "entity-operator"),
                new NamedType(ClusterOperatorIdentity.class, "cluster-operator"),
                new NamedType(KafkaExporterIdentity.class, "kafka-exporter"));
    }

    private final Clock clock;
    private final KubernetesClient client;
    private final CertManager certManager;

    public ClusterPki(Clock clock,
                      KubernetesClient client,
                      CertManager certManager) {
        this.clock = Objects.requireNonNull(clock);
        this.client = Objects.requireNonNull(client);
        this.certManager = certManager;

    }

    public static class ClusterCerts {
        private final Reconciliation reconciliation;
        private final CertificateController controller;
        private final TrustManager trustManager;
        private final UseManager useManager;
        private final String namespace;
        private final String cluster;
        private final ZookeeperCertificateOutputWriter zkCertsWriter;
        private final BrokersCertificateOutputWriter kafkaCertsWriter;
        private final AbstractCertificateOutputWriter<EntityOperatorIdentity> eoCertWriter;
        private final AbstractCertificateOutputWriter<KafkaExporterIdentity> exporterCertWriter;
        private final AbstractCertificateOutputWriter<CruiseControlIdentity> ccCertWriter;
        private final AbstractCertificateOutputWriter<ClusterOperatorIdentity> coCertWriter;

        public ClusterCerts(KubernetesClient client,
                            TrustController2 useManager,
                            Reconciliation reconciliation,
                            CertificateController controller,
                            String namespace,
                            String cluster,
                            OwnerReference ownerReference) {
            this.reconciliation = reconciliation;
            this.controller = controller;

            this.trustManager = useManager;
            this.useManager = useManager;
            this.namespace = namespace;
            this.cluster = cluster;
            this.zkCertsWriter = new ZookeeperCertificateOutputWriter(reconciliation, cluster,
                    new NamedResource<>(client.secrets().inNamespace(namespace), ZookeeperCluster.nodesSecretName(cluster)),
                    new NamedResource<>(client.secrets().inNamespace(namespace), ZookeeperCluster.nodesSecretName(cluster)),
                    ownerReference);
            this.kafkaCertsWriter = new BrokersCertificateOutputWriter(reconciliation, cluster,
                    new NamedResource<>(client.secrets().inNamespace(namespace), KafkaCluster.brokersSecretName(cluster)),
                    new NamedResource<>(client.secrets().inNamespace(namespace), KafkaCluster.brokersSecretName(cluster)),
                    ownerReference, true, true);
            this.eoCertWriter = new AbstractCertificateOutputWriter<>(reconciliation,
                    "entity-operator",
                    ownerReference, MAPPER) {
                @Override
                protected String stateKey(EntityOperatorIdentity id) {
                    return "entity-operator.state";
                }

                @Override
                protected NamedResource<Secret> statusResource(EntityOperatorIdentity entityOperatorIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), KafkaResources.entityOperatorSecretName(cluster));
                }

                @Override
                protected NamedResource<Secret> issuedResource(EntityOperatorIdentity entityOperatorIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), KafkaResources.entityOperatorSecretName(cluster));
                }
            };
            this.exporterCertWriter = new AbstractCertificateOutputWriter<>(reconciliation,
                    "kafka-exporter",
                    ownerReference, MAPPER) {
                @Override
                protected String stateKey(KafkaExporterIdentity id) {
                    return "kafka-exporter.state";
                }

                @Override
                protected NamedResource<Secret> statusResource(KafkaExporterIdentity kafkaExporterIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), KafkaExporterResources.secretName(cluster));
                }

                @Override
                protected NamedResource<Secret> issuedResource(KafkaExporterIdentity kafkaExporterIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), KafkaExporterResources.secretName(cluster));
                }
            };
            this.ccCertWriter = new AbstractCertificateOutputWriter<>(reconciliation,
                    "cruise-control",
                    ownerReference, MAPPER) {
                @Override
                protected NamedResource<Secret> issuedResource(CruiseControlIdentity id) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), CruiseControl.secretName(cluster));
                }

                @Override
                protected NamedResource<Secret> statusResource(CruiseControlIdentity id) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), CruiseControl.secretName(cluster));
                }

                @Override
                protected String stateKey(CruiseControlIdentity id) {
                    return "cruise-control.state";
                }
            };
            this.coCertWriter = new AbstractCertificateOutputWriter<>(reconciliation,
                    "cluster-operator",
                    ownerReference, MAPPER) {
                @Override
                protected String stateKey(ClusterOperatorIdentity id) {
                    return "cluster-operator.state";
                }

                @Override
                protected NamedResource<Secret> statusResource(ClusterOperatorIdentity clusterOperatorIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), ClusterOperator.secretName(cluster));
                }

                @Override
                protected NamedResource<Secret> issuedResource(ClusterOperatorIdentity clusterOperatorIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), ClusterOperator.secretName(cluster));
                }
            };

        }

        public List<CertificateReconcileResult> issueZkCertificates(Kafka kafka) throws CertificateException {
            long t0 = System.nanoTime();
            // possibly create a bunch of pending certificates
            int zkReplicas = kafka.getSpec().getZookeeper().getReplicas() + 1;
            List<CertificateReconcileResult> results = new ArrayList<>(zkReplicas);
            for (int podId = 0; podId < zkReplicas - 1; podId++) {
                CertificateReconcileResult reconcile = controller.reconcile(zkSubject(namespace, cluster, podId), zkCertsWriter);

                // TODO only need to do this if it's not already trusted.
                reconcile.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
                results.add(reconcile);
            }
            LOGGER.debugCr(reconciliation, "ZK certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
            return results;
        }

        public List<CertificateReconcileResult> issueKafkaCertificates(Kafka kafka,
                                           Set<String> externalBootstrapAddresses,
                                           Map<Integer, Set<String>> externalAddresses) throws CertificateException {
            Objects.requireNonNull(this.controller);
            long t0 = System.nanoTime();
            int kafkaReplicas = kafka.getSpec().getKafka().getReplicas();
            List<CertificateReconcileResult> results = new ArrayList<>(kafkaReplicas);
            for (int brokerId = 0; brokerId < kafkaReplicas; brokerId++) {
                CertificateReconcileResult reconcile = controller.reconcile(
                        brokerSubject(namespace, cluster, brokerId,
                                externalBootstrapAddresses, externalAddresses),
                        kafkaCertsWriter);
                reconcile.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
                results.add(reconcile);
            }
            LOGGER.debugCr(reconciliation, "Kafka certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
            return results;
        }

        public CertificateReconcileResult issueEoCertificate(Kafka kafka) throws CertificateException {
            io.strimzi.api.kafka.model.EntityOperatorSpec entityOperator = kafka.getSpec().getEntityOperator();
            if (entityOperator != null) {
                long t0 = System.nanoTime();

                CertificateReconcileResult result = controller.reconcile(new RequiredCertificate<>(new EntityOperatorIdentity(),
                                new Subject.Builder()
                                        .withCommonName(entityOperatorName(cluster))
                                        .withOrganizationName(Ca.IO_STRIMZI)
                                        .build()),
                        eoCertWriter);
                result.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
                LOGGER.debugCr(reconciliation, "EO certificate issuance took {}ms",
                        (System.nanoTime() - t0) / 1_000_000.0);
                return result;
            }
            return null;
        }

        public CertificateReconcileResult issueKafkaExporterCertificates(Kafka kafka) throws CertificateException {

            io.strimzi.api.kafka.model.KafkaExporterSpec i = kafka.getSpec().getKafkaExporter();
            if (i != null) {
                long t0 = System.nanoTime();

                CertificateReconcileResult result = controller.reconcile(new RequiredCertificate<>(new KafkaExporterIdentity(),
                                new Subject.Builder()
                                        .withCommonName(KafkaExporterResources.deploymentName(cluster))
                                        .withOrganizationName(Ca.IO_STRIMZI)
                                        .build()),
                        exporterCertWriter);
                result.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
                LOGGER.debugCr(reconciliation, "Kafka Exporter certificate issuance took {}ms",
                        (System.nanoTime() - t0) / 1_000_000.0);
                return result;
            }
            return null;
        }

        public CertificateReconcileResult issueCcCertificate(Kafka kafka) throws CertificateException {
            if (kafka.getSpec().getCruiseControl() != null) {
                // TODO collect all the certs which we're waiting for trust on
                long t0 = System.nanoTime();
                CertificateReconcileResult result = controller.reconcile(new RequiredCertificate<>(new CruiseControlIdentity(),
                                new Subject.Builder()
                                        .withCommonName(CruiseControlResources.deploymentName(cluster))
                                        .withOrganizationName(Ca.IO_STRIMZI)
                                        .build()),
                        ccCertWriter);
                result.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
                LOGGER.debugCr(reconciliation, "Cruise Control certificate issuance took {}ms",
                        (System.nanoTime() - t0) / 1_000_000.0);
                return result;
            }
            return null;
        }

        public CertificateReconcileResult issueCoCertificate(Kafka kafka) throws CertificateException {
            long t0 = System.nanoTime();
            CertificateReconcileResult result = controller.reconcile(new RequiredCertificate<>(new ClusterOperatorIdentity(),
                            new Subject.Builder()
                                    .withCommonName("cluster-operator")
                                    .withOrganizationName(Ca.IO_STRIMZI)
                                    .build()),
                    coCertWriter);
            result.trustRoot().ifPresent(cert -> useManager.requireTrust(cert));
            LOGGER.debugCr(reconciliation, "Cluster Operator certificate issuance took {}ms",
                    (System.nanoTime() - t0) / 1_000_000.0);
            return result;
            // TODO It will figure out the unused certs too, somehow.
            // It should return something which we can feed to the KafkaRoller predicate
            // And also ZK etc.
        }
    }

    /**
     * Create the CA according to the certificateAuthority
     * @param clusterCa The Kafka CR CA.
     * @param cluster The name of the cluster.
     * @param namespace The namespace of the cluster.
     * @param ownerReference The owner reference for the secrets.
     * @param reconciliation The reconciliation.
     * @throws CertificateException if problems.
     * @return A result
     */
    public ClusterCerts bootstrapCa(Reconciliation reconciliation,
                                    String namespace, String cluster,
                                    CertificateAuthority clusterCa,
                                    OwnerReference ownerReference) throws CertificateException {
        Objects.requireNonNull(namespace);
        Objects.requireNonNull(cluster);
        if (clusterCa == null) {
            clusterCa = new CertificateAuthority();
            clusterCa.setValidityDays(365);
            clusterCa.setRenewalDays(30);
        }
        long t0 = System.nanoTime();
        Chain chain = new Chain(reconciliation, clock,
                client.secrets().inNamespace(namespace),
                certManager);

        // TODO fix the name
//        TrustPrivateState trustPrivateState = new TrustPrivateState(
//                ownerReference,
//                new NamedResource<>(client.secrets().inNamespace(namespace), cluster + "-cluster-pki-state"));
        TrustController2 useManager = new TrustController2(reconciliation,
                new NamedResource<>(client.secrets().inNamespace(namespace), cluster + "-cluster-pki-state"));
        CertificateIssuer issuer = chain.reconcile(Chain.clusterCaFromCrd(cluster, clusterCa),
                clusterCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
        Validator validator = new Validator(clock, clusterCa.getRenewalDays());
        CertificateController controller = new CertificateController(reconciliation,
                useManager,
                issuer,
                validator,
                clusterCa.getValidityDays());
        return new ClusterCerts(client,
                useManager,
                reconciliation,
                controller,
                namespace,
                cluster,
                ownerReference);
    }

    static RequiredCertificate<BrokerIdentity> brokerSubject(String namespace,
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
        return new RequiredCertificate<>(new BrokerIdentity(brokerId), subject.build());

    }

    static RequiredCertificate<ZooKeeperNodeIdentity> zkSubject(String namespace,
                     String cluster,
                     int podId) {
        DnsNameGenerator zkDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.serviceName(cluster));
        DnsNameGenerator zkHeadlessDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.headlessServiceName(cluster));

        Subject.Builder subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
        subject.addDnsName(ZookeeperCluster.serviceName(cluster));
        subject.addDnsName(String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
        subject.addDnsName(zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkDnsGenerator.serviceDnsName());
        subject.addDnsName(ZookeeperCluster.podDnsName(namespace, cluster, podId));
        subject.addDnsName(ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, podId));
        subject.addDnsName(zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkDnsGenerator.wildcardServiceDnsName());
        subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
        subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsName());
        return new RequiredCertificate<>(new ZooKeeperNodeIdentity(podId), subject.build());
    }

    // TODO this needs a test
    public void trust(ClusterCerts result) throws CertificateEncodingException {
        // We should get the zombies and the requiringTrust after issuing, before rolling
        // and then pass them after rolling to this method.
        result.trustManager.haveTrust(result.trustManager.requiringTrust());
        result.trustManager.delete(result.trustManager.zombies());

        ////////////////////////////////////////////////////////////////////////////////////

        // TODO adding usedTrustRoots doesn't solve the problem for the UO though does it?
        // We still have the situation where the CO (TC) doesn't know which root certs can be removed because it's the UO (CC) which knows about the EE certs
        // We need some kind of shared state.
        // For example if the Uo, on a full reconciliation, recorded a count for each root cert of the number of live EE certs
        // <root cert fingerprint>: <count of ee certs>
        // 1234567890abcdef: 4
        // abcdef1234567890: 2
        // (this could be part of the trust state)
        // Then the CO could see what states each of these root certs was in
        // When the state was PHASE_OUT and the count is zero we can transition to ZOMBIE be do the rolling to remove from trust


//        TrustController trustController = new TrustController(result.reconciliation,
//                result.trustPrivateState,
//                List.of(new LegacyTrustOutputWriter(client.secrets().inNamespace(result.namespace)
//                        .withName(KafkaResources.clusterCaCertificateSecretName(result.cluster)))));

//
//        Comparator<TrustRoot> comparator = Comparator.comparing(TrustRoot::fingerprint);
//        Set<TrustRoot> newState = result.trustPrivateState.load().values().stream().map(trustRoot -> {
//            if (trustRoot.state() == TrustState.UNTRUSTED) {
//                // TODO this is fishy:
//                // a) what is enforcing the valid transitions
//                // b) why the need for the resourceVersion argument, then the TrustRoot already knows its rv?
//                // c) why is it only toTrusted that needs the rv?
//                return trustRoot.toTrusted(trustRoot.resourceVersion());
//            } else {
//                return trustRoot;
//            }
//        }).collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
//        Set<TrustRoot> toPhaseOut = new TreeSet<>(comparator);
//        // TODO Here we want to know all the EE certs in CertificateReconcileResult.WaitingForTrust, OkIssued or OkNoop
//        // I.e. Root certs with state PENDING or IN_USE
//        // In kube we could do this using a label, allowing us to select by status
//        // But vault doesn't really support a usable LIST (https://www.vaultproject.io/api-docs/secret/kv/kv-v1)
//        // with selection semantic. The best we could do would be to LIST.map(GET).filter(STATE)
//        toPhaseOut.removeAll(usedTrustRoots);
//        newState.addAll(toPhaseOut.stream().map(TrustRoot::toPhaseOut).collect(Collectors.toSet()));
//        // If a new CA cert was used this will
//        trustController.reconcile(newState);
    }
}
