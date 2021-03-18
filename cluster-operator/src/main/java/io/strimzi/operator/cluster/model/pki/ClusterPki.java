/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.status.CaCertStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ZookeeperCluster;

public class ClusterPki extends Pki<Kafka, KafkaSpec, KafkaStatus> {

    private static final Pattern IPV4_ADDRESS = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");

    public ClusterPki(Clock clock, CertManager certManager, KubernetesClient client, String namespace, String cluster) {
        super(clock, certManager, client, namespace, cluster,
                KafkaResources.clusterCaKeySecretName(cluster),
                KafkaResources.clusterCaCertificateSecretName(cluster)
        );
    }

    @Override
    protected CertificateAuthority ca(Kafka kafka) {
        return kafka.getSpec().getClusterCa();
    }

    @Override
    protected Stream<RequiredCertificate> requiredCertificates(Kafka cr) {
        return Stream.concat(
                requiredZookeeperCertificates(cr),
                requiredBrokerCertificates(cr.getSpec().getKafka()));
    }

    @Override
    protected List<CaCertStatus> status(Kafka kafka) {
        return kafka == null || kafka.getStatus() == null ? null : kafka.getStatus().getClusterCa();
    }

    @Override
    protected Kafka updateStatus(Kafka kafka, List<CaCertStatus> statusList) {
        Resource<Kafka> kafkaRef = Crds.kafkaOperation(client).inNamespace(namespace).withName(cluster);
        return kafkaRef.updateStatus(new KafkaBuilder(kafkaRef.get()).editOrNewStatus()
                .withClusterCa(statusList).endStatus().build());
    }

    Stream<RequiredCertificate> requiredZookeeperCertificates(Kafka cr) {
        return IntStream.range(0, cr.getSpec().getZookeeper().getReplicas()).boxed()
                .map(podNum -> {
                    DnsNameGenerator zkDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.serviceName(cluster));
                    DnsNameGenerator zkHeadlessDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.headlessServiceName(cluster));

                    Subject.Builder subject = new Subject.Builder()
                            .withOrganizationName("io.strimzi")
                            .withCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
                    subject.addDnsName(ZookeeperCluster.serviceName(cluster));
                    subject.addDnsName(String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
                    subject.addDnsName(zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
                    subject.addDnsName(zkDnsGenerator.serviceDnsName());
                    subject.addDnsName(ZookeeperCluster.podDnsName(namespace, cluster, podNum));
                    subject.addDnsName(ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, podNum));
                    subject.addDnsName(zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
                    subject.addDnsName(zkDnsGenerator.wildcardServiceDnsName());
                    subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
                    subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsName());

//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".key", cert.keyAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".crt", cert.certAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());

                    return new RequiredCertificate(subject.build(),
                            ZookeeperCluster.nodesSecretName(cluster),
                            zookeeperNodeCertSecretEntry(cluster, podNum));
                });
    }

    Stream<RequiredCertificate> requiredBrokerCertificates(KafkaClusterSpec kafkaSpec) {
        return IntStream.range(0, kafkaSpec.getReplicas()).boxed()
                .map(podNum -> {
                    Set<String> externalBootstrapAddresses = null; // TODO
                    Map<Integer, Set<String>> externalAddresses = null; // TODO

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
                    subject.addDnsName(KafkaCluster.podDnsName(namespace, cluster, podNum));
                    subject.addDnsName(KafkaCluster.podDnsNameWithoutClusterDomain(namespace, cluster, podNum));

                    if (externalBootstrapAddresses != null)   {
                        for (String dnsName : externalBootstrapAddresses) {
                            if (!IPV4_ADDRESS.matcher(dnsName).matches()) {
                                subject.addDnsName(dnsName);
                            } else {
                                subject.addIpAddress(dnsName);
                            }
                        }
                    }

                    if (externalAddresses != null && externalAddresses.get(podNum) != null)   {
                        for (String dnsName : externalAddresses.get(podNum)) {
                            if (!IPV4_ADDRESS.matcher(dnsName).matches()) {
                                subject.addDnsName(dnsName);
                            } else {
                                subject.addIpAddress(dnsName);
                            }
                        }
                    }

//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".key", cert.keyAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".crt", cert.certAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());
                    return new RequiredCertificate(subject.build(),
                            KafkaCluster.brokersSecretName(cluster),
                            brokerCertSecretEntry(cluster, podNum));
                });
    }

    static String brokerCertSecretEntry(String cluster, Integer podNum) {
        return KafkaCluster.kafkaPodName(cluster, podNum) + ".crt";
    }

    static String zookeeperNodeCertSecretEntry(String cluster, Integer podNum) {
        return ZookeeperCluster.zookeeperPodName(cluster, podNum) + ".crt";
    }

}
