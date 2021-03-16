/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.HashMap;
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

    public ClusterPki(CertManager certManager, KubernetesClient client, String namespace, String cluster) {
        super(certManager, client, namespace, cluster,
                KafkaResources.clusterCaKeySecretName(cluster),
                KafkaResources.clusterCaCertificateSecretName(cluster));
    }

    @Override
    protected CertificateAuthority ca(Kafka kafka) {
        return kafka.getSpec().getClusterCa();
    }

    @Override
    protected Stream<RequiredCertificate> requiredCertificates(Kafka cr) {
        Stream<RequiredCertificate> requiredCertificates = Stream.concat(
                requiredBrokerCertificates(cr.getSpec().getKafka()),
                requiredZookeeperCertificates(cr));
        return requiredCertificates;
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

                    Map<String, String> sbjAltNames = new HashMap<>(6);
                    sbjAltNames.put("DNS.1", ZookeeperCluster.serviceName(cluster));
                    sbjAltNames.put("DNS.2", String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
                    sbjAltNames.put("DNS.3", zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
                    sbjAltNames.put("DNS.4", zkDnsGenerator.serviceDnsName());
                    sbjAltNames.put("DNS.5", ZookeeperCluster.podDnsName(namespace, cluster, podNum));
                    sbjAltNames.put("DNS.6", ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, podNum));
                    sbjAltNames.put("DNS.7", zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
                    sbjAltNames.put("DNS.8", zkDnsGenerator.wildcardServiceDnsName());
                    sbjAltNames.put("DNS.9", zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
                    sbjAltNames.put("DNS.10", zkHeadlessDnsGenerator.wildcardServiceDnsName());

                    Subject subject = new Subject();
                    subject.setOrganizationName("io.strimzi");
                    subject.setCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
                    subject.setSubjectAltNames(sbjAltNames);

//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".key", cert.keyAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".crt", cert.certAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
//                    data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());

                    return new RequiredCertificate(subject,
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

                    Map<String, String> sbjAltNames = new HashMap<>();
                    sbjAltNames.put("DNS.1", KafkaCluster.serviceName(cluster));
                    sbjAltNames.put("DNS.2", String.format("%s.%s", KafkaCluster.serviceName(cluster), namespace));
                    sbjAltNames.put("DNS.3", kafkaDnsGenerator.serviceDnsNameWithoutClusterDomain());
                    sbjAltNames.put("DNS.4", kafkaDnsGenerator.serviceDnsName());
                    sbjAltNames.put("DNS.5", KafkaCluster.headlessServiceName(cluster));
                    sbjAltNames.put("DNS.6", String.format("%s.%s", KafkaCluster.headlessServiceName(cluster), namespace));
                    sbjAltNames.put("DNS.7", kafkaHeadlessDnsGenerator.serviceDnsNameWithoutClusterDomain());
                    sbjAltNames.put("DNS.8", kafkaHeadlessDnsGenerator.serviceDnsName());
                    sbjAltNames.put("DNS.9", KafkaCluster.podDnsName(namespace, cluster, podNum));
                    sbjAltNames.put("DNS.10", KafkaCluster.podDnsNameWithoutClusterDomain(namespace, cluster, podNum));
                    int nextDnsId = 11;
                    int nextIpId = 1;

                    if (externalBootstrapAddresses != null)   {
                        for (String dnsName : externalBootstrapAddresses) {
                            String sna = !IPV4_ADDRESS.matcher(dnsName).matches() ?
                                    String.format("DNS.%d", nextDnsId++) :
                                    String.format("IP.%d", nextIpId++);

                            sbjAltNames.put(sna, dnsName);
                        }
                    }

                    if (externalAddresses != null && externalAddresses.get(podNum) != null)   {
                        for (String dnsName : externalAddresses.get(podNum)) {
                            String sna = !IPV4_ADDRESS.matcher(dnsName).matches() ?
                                    String.format("DNS.%d", nextDnsId++) :
                                    String.format("IP.%d", nextIpId++);

                            sbjAltNames.put(sna, dnsName);
                        }
                    }

                    Subject subject = new Subject();
                    subject.setOrganizationName("io.strimzi");
                    subject.setCommonName(KafkaCluster.kafkaClusterName(cluster));
                    subject.setSubjectAltNames(sbjAltNames);

//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".key", cert.keyAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".crt", cert.certAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
//                    data.put(KafkaCluster.kafkaPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());
                    return new RequiredCertificate(subject,
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
