/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.CaCertState;
import io.strimzi.api.kafka.model.status.CaCertStatus;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.pki.TrustManager.CaState;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


class ClusterPkiTest {

    public static final String NAMESPACE = "namespace";
    public static final String CLUSTER = "my-cluster";
    private KubernetesClient client;
    private Subject subject;
    private Kafka cr;
    private Resource<Kafka> kafkaRef;

    @BeforeEach
    public void before() {
        client = new MockKube().withCustomResourceDefinition(Crds.kafka(), Kafka.class, KafkaList.class, Kafka::getStatus, Kafka::setStatus).end().build();
        subject = new Subject();
        subject.setCommonName("foo");
        subject.setOrganizationName("bar");
        subject.setSubjectAltNames(Map.of("DNS.1", "gee"));
        kafkaRef = Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER);
        cr = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(CLUSTER)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterCa()
                        .withGenerateCertificateAuthority(true)
                        .withValidityDays(10)
                    .endClusterCa()
                    .withNewKafka()
                        .withReplicas(3)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                .endSpec()
            .build();
        kafkaRef.create(cr);
    }

    // TODO bootstrap case
    // TODO recover cases
    // TODO normal case

    /**
     * Test the boot strap case, where the secrets not yet exist
     * @throws CertificateException
     */
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

    @Test
    public void testBootstrapReconcile() throws CertificateException {
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        TrustManager trustManager = pki.loadTrust(cr);
        doBothRconcilePhases(trustManager, pki);
    }

    void doBothRconcilePhases(TrustManager trustManager, Pki pki) throws CertificateException {

        // TODO we can't use a different trust manager here than is used by reconcile

        pki.reconcile(cr, trustManager);

        List<CaCertStatus> clusterCa = kafkaRef.get().getStatus().getClusterCa();
        System.err.println(clusterCa);
        dumpSecrets();

        // assert the secrets
        assertCaSecret(trustManager);
        assertTrustSecret(trustManager);
        // assert the Kafka status
        assertEquals(trustManager.asStatusList(), clusterCa);
        assertEquals(1, clusterCa.size());
        assertEquals(CaCertState.UNTRUSTED, clusterCa.get(0).getState());

        // TODO assert that each of the secrets has a .tmp file

        pki.reconcile2(cr, trustManager);

        // TODO assert that none of the client secrets has a .tmp file
    }

    @Test
    public void testSubsequentReconcile() throws CertificateException {
        // Do an initial reconcile
        TrustManager trustManager = new TrustManager();
        ClusterPki pki = new ClusterPki(new OpenSslCertManager(), client, NAMESPACE, CLUSTER);
        doBothRconcilePhases(trustManager, pki);

        // Do a subsequent reconcile
        trustManager = pki.loadTrust(kafkaRef.get());
        // TODO need to assert that certs not regenerated on 2nd reconcile
        // Can we do this using mocks?
        doBothRconcilePhases(trustManager, pki);
    }

    @Test
    public void testRecoverFromDeletedBrokerSecret() throws CertificateException {
        // TODO
        fail("TODO");
    }

    @Test
    public void testRecoverFromDeletedCaSecret() throws CertificateException {
        // TODO
        fail("TODO");
    }

    @Test
    public void testRecoverFromDeletedCertSecret() throws CertificateException {
        // TODO
        fail("TODO");
    }

    @Test
    public void testLeafCertWillExpire() throws CertificateException {
        // TODO
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

    void assertCaSecret(TrustManager trustManager) {
        CaState caState = trustManager.findCa(CaCertState.UNTRUSTED).get();

        assertEquals(Set.of(caState.fingerprint() + ".key"), client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER)).get().getData().keySet());
    }

    void assertTrustSecret(TrustManager trustManager) {
        assertEquals(trustManager.entries().stream().map(x -> x.fingerprint() + "." + x.state() + ".crt").collect(Collectors.toSet()),
                client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER)).get().getData().keySet());
    }

    void dumpSecrets() {
        client.secrets().list().getItems().forEach(s -> System.err.println("Secret " + s.getMetadata().getName() + " has keys " + s.getData().keySet()));
    }

}