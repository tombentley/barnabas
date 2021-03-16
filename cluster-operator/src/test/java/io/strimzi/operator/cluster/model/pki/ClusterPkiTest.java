/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.CaCertState;
import io.strimzi.api.kafka.model.status.CaCertStatus;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.pki.TrustManager.CaState;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


class ClusterPkiTest {

    public static final String NAMESPACE = "namespace";
    public static final String CLUSTER = "my-cluster";
    public static final String CA_CERTS_SECRET = KafkaResources.clusterCaCertificateSecretName(CLUSTER);
    public static final String CA_KEY_SECRET = KafkaResources.clusterCaKeySecretName(CLUSTER);
    private KubernetesClient client;
    private Resource<Kafka> kafkaRef;

    @BeforeEach
    public void before() {
        client = new MockKube().withCustomResourceDefinition(Crds.kafka(), Kafka.class, KafkaList.class, Kafka::getStatus, Kafka::setStatus).end().build();
        Subject subject = new Subject();
        subject.setCommonName("foo");
        subject.setOrganizationName("bar");
        subject.setSubjectAltNames(Map.of("DNS.1", "gee"));
        kafkaRef = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER);
    }

    // TODO bootstrap case
    // TODO recover cases
    // TODO normal case

//    /**
//     * Test the boot strap case, where the secrets not yet exist
//     * @throws CertificateException
//     */
//    @Test
//    public void testBootstrap() throws CertificateException {
//        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
//        TrustManager trustManager = new TrustManager();
//
//        pki.issueClientCertificate(cr, trustManager, subject, 100);
//
//        List<CaCertStatus> clusterCa = kafkaRef.get().getStatus().getClusterCa();
//        // assert the secrets
//        assertCaSecret();
//        assertTrustSecret(trustManager);
//        // assert the Kafka status
//        List<CaCertStatus> clusterCa1 = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER).get().getStatus().getClusterCa();
//        assertEquals(trustManager.asStatusList(),
//                clusterCa1);
//        assertEquals(1, clusterCa1.size());
//        assertEquals(CaCertState.UNTRUSTED, clusterCa1.get(0).getState());
//
//        System.out.println(clusterCa);
//        dumpSecrets();
//    }
//
//    @Test
//    public void testLoadStateFromSecret() throws CertificateException {
//        testBootstrap();
//        // TODO
//    }

    static Stream<Kafka> crs() {
        return Stream.of(
                new CertificateAuthorityBuilder()
                    .withGenerateCertificateAuthority(true)
                    .withValidityDays(10)
//                , TODO cover all the things
//                new CertificateAuthorityBuilder()
//                    .withGenerateCertificateAuthority(true)
//                    .withValidityDays(10)
            ).map(ca ->
                new KafkaBuilder()
                    .withNewMetadata()
                        .withNamespace(NAMESPACE)
                        .withName(CLUSTER)
                    .endMetadata()
                    .withNewSpec()
                        .withClusterCa(ca.build())
                        .withNewKafka()
                            .withReplicas(3)
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(3)
                        .endZookeeper()
                    .endSpec()
                    .build()
        );
    }

    @ParameterizedTest
    @MethodSource("crs")
    public void testBootstrapReconcile(Kafka cr) throws CertificateException, UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException, IOException {
        kafkaRef.create(cr);
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        TrustManager trustManager = pki.loadTrust(cr);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);
    }

    void doBothReconcilePhases(Kafka cr, TrustManager trustManager, Pki pki, CaCertState expectedState) throws CertificateException, NoSuchAlgorithmException, UnrecoverableEntryException, KeyStoreException, IOException {
        doPhase1(cr, trustManager, pki, expectedState);
        doPhase2(cr, trustManager, pki);
    }

    private void doPhase2(Kafka cr, TrustManager trustManager, Pki pki) throws CertificateException, UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException, IOException {
        List<CaCertStatus> clusterCa;
        CaState caState;
        // TODO EO, KafkaExporter, CO secret

        pki.reconcile2(cr, trustManager);

        // assert the Kafka status
        clusterCa = kafkaRef.get().getStatus().getClusterCa();
        caState = trustManager.findCa(CaCertState.TRUSTED).get();
        assertEquals(
                trustManager.asStatusList(),
                clusterCa,
                "Expected trust manager state to match CR status state");
        assertEquals(1,
                clusterCa.size(),
                "Expected a single certificate in CR status");
        assertEquals(CaCertState.TRUSTED,
                clusterCa.get(0).getState(),
                "Expected CR status's certificate to be in the UNTRUSTED state");
        // assert that none of the client secrets has a .tmp file
        assertEquals(IntStream.range(0, cr.getSpec().getZookeeper().getReplicas()).boxed()
                .flatMap(podNum ->
                        Stream.of(ClusterPki.zookeeperNodeCertSecretEntry(CLUSTER, podNum),
                                ClusterPki.zookeeperNodeCertSecretEntry(CLUSTER, podNum).replaceAll("\\.crt$", ".key"))
                ).collect(Collectors.toSet()),
                secretItemKeys(ZookeeperCluster.nodesSecretName(CLUSTER)),
                "Expected a .crt secret entry for each zookeeper node");
        assertEquals(IntStream.range(0, cr.getSpec().getKafka().getReplicas()).boxed()
                .flatMap(podNum ->
                        Stream.of(ClusterPki.brokerCertSecretEntry(CLUSTER, podNum),
                                ClusterPki.brokerCertSecretEntry(CLUSTER, podNum).replaceAll("\\.crt$", ".key"))
                ).collect(Collectors.toSet()),
                secretItemKeys(KafkaCluster.brokersSecretName(CLUSTER)),
                "Expected a .crt secret entry for each broker");
        // TODO EO, KafkaExporter, CO secret
    }

    private void doPhase1(Kafka cr, TrustManager trustManager, Pki pki, CaCertState expectedState) throws CertificateException {
        assertEquals(expectedState == CaCertState.UNTRUSTED, pki.reconcile(cr, trustManager),
                "Expected reconcile1 to require a rolling deployment of new CA certs");

        List<CaCertStatus> clusterCa = kafkaRef.get().getStatus().getClusterCa();
        System.err.println(clusterCa);
        dumpSecrets();

        // assert the secrets
        CaState caState = trustManager.findCa(expectedState).get();
        assertEquals(
                Set.of(caState.fingerprint() + ".key", "pending"),
                secretItemKeys(CA_KEY_SECRET),
                "Expected a single item in the " + CA_KEY_SECRET + " secret");
        assertEquals(
                trustManager.entries().stream().map(x -> x.fingerprint() + "." + x.state() + ".crt").collect(Collectors.toSet()),
                secretItemKeys(CA_CERTS_SECRET),
                "Expected trust manager certs to match the items in " + CA_CERTS_SECRET);
        // assert the Kafka status
        assertEquals(
                trustManager.asStatusList(),
                clusterCa,
                "Expected trust manager state to match CR status state");
        assertEquals(1,
                clusterCa.size(),
                "Expected a single certificate in CR status");
        assertEquals(expectedState,
                clusterCa.get(0).getState(),
                "Expected CR status's certificate to be in the UNTRUSTED state");

        if (expectedState == CaCertState.UNTRUSTED) {
            assertEquals(IntStream.range(0, cr.getSpec().getZookeeper().getReplicas()).boxed().map(
                    podNum -> ClusterPki.zookeeperNodeCertSecretEntry(CLUSTER, podNum) + RequiredCertificate.PENDING_SUFFIX).collect(Collectors.toSet()),
                    secretItemKeys(ZookeeperCluster.nodesSecretName(CLUSTER)),
                    "Expected a .crt.tmp secret entry for each zookeeper node");
            assertEquals(IntStream.range(0, cr.getSpec().getKafka().getReplicas()).boxed().map(
                    podNum -> ClusterPki.brokerCertSecretEntry(CLUSTER, podNum) + RequiredCertificate.PENDING_SUFFIX).collect(Collectors.toSet()),
                    secretItemKeys(KafkaCluster.brokersSecretName(CLUSTER)),
                    "Expected a .crt.tmp secret entry for each broker");
        }
    }

    @ParameterizedTest
    @MethodSource("crs")
    public void testSubsequentReconcile(Kafka cr) throws CertificateException, UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException, IOException {
        kafkaRef.create(cr);
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        // TODO need to assert that certs not regenerated on 2nd reconcile
        // Can we do this using mocks?
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.TRUSTED);
    }

    @Test
    public void testLeafCertWillExpire() throws CertificateException {
        // TODO create all the secrets just right
        // TODO do a reconcile
        fail("TODO");
    }

    @Test
    public void testIntermediateCaCertWillExpire() throws CertificateException {
        // TODO
        fail("TODO");
    }

    @Test
    public void testRootCaCertWillExpire() throws CertificateException {
        // TODO
        fail("TODO");
    }

    /** Test what happens if the broker's Secret get deleted */
    @ParameterizedTest
    @MethodSource("crs")
    public void testRecoverFromDeletedBrokerSecret(Kafka cr) throws CertificateException, NoSuchAlgorithmException, UnrecoverableEntryException, KeyStoreException, IOException {
        kafkaRef.create(cr);
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);

        // Now delete the secret
        client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER)).delete();

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.TRUSTED);
    }

    /** Test what happens if the broker's Secret get deleted */
    @ParameterizedTest
    @MethodSource("crs")
    public void testRecoverFromDeletedZookeeperNodesSecret(Kafka cr) throws CertificateException, NoSuchAlgorithmException, UnrecoverableEntryException, KeyStoreException, IOException {
        kafkaRef.create(cr);
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);

        // Now delete the secret
        client.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER)).delete();

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.TRUSTED);

    }

    @ParameterizedTest
    @MethodSource("crs")
    public void testRecoverFromDeletedCaKeySecret(Kafka cr) throws CertificateException, UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException, IOException {
        fail("Actually what is the right thing here? We don't actually care until we come to need a certificate, right?");
        kafkaRef.create(cr);
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);

        // Now delete the secret
        client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER)).delete();

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);
    }

    @ParameterizedTest
    @MethodSource("crs")
    public void testRecoverFromDeletedCaCertSecret(Kafka cr) throws CertificateException, UnrecoverableEntryException, NoSuchAlgorithmException, KeyStoreException, IOException {
        fail("I think in this case we're screwed because (no matter which CA) Kafka brokers won't be able to accept connections from either other brokers or from clients");
        // So we need to reissue all the client certs, but (for the client's CA) we don't know what all of those are.
        kafkaRef.create(cr);
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);

        // Now delete the secret
        client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER)).delete();

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        doBothReconcilePhases(cr, trustManager, pki, CaCertState.UNTRUSTED);
    }


    private Set<String> secretItemKeys(String secretName) {
        return client.secrets().inNamespace(NAMESPACE).withName(secretName).get().getData().keySet();
    }

    void dumpSecrets() {
        client.secrets().list().getItems().forEach(s -> System.err.println("Secret " + s.getMetadata().getName() + " has keys " + s.getData().keySet()));
    }

}