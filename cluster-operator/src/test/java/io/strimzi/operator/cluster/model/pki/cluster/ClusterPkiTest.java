/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.CertificateState;
import io.strimzi.operator.cluster.model.pki.Fingerprint;
import io.strimzi.operator.cluster.model.pki.PkiUtils;
import io.strimzi.operator.cluster.model.pki.TestClock;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ClusterPkiTest {

    public static final String NAMESPACE = "ns";
    public static final String CLUSTER = "my-cluster";
    private TestClock clock;
    private KubernetesClient client;
    private OpenSslCertManager certManager;
    private ClusterPki pki;
    private Kafka kafka;
    private OwnerReference ownerReference;

    @BeforeEach
    public void before() {
        clock = TestClock.EPOCH;
        client = new MockKube().build();
        certManager = new OpenSslCertManager(clock);
        pki = new ClusterPki(clock, client, certManager);
        kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(CLUSTER)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterCa()
                        .withValidityDays(30)
                        .withRenewalDays(5)
                    .endClusterCa()
                .endSpec()
            .build();
        ownerReference = null;
    }

    @Test
    public void testClusterCreation() throws CertificateException {
        clusterCreation();
    }
    public void clusterCreation() throws CertificateException {
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", NAMESPACE, CLUSTER);

        // (1) Bootstrap the CA
        final ClusterPki.ClusterCerts clusterCerts = pki.bootstrapCa(reconciliation, NAMESPACE, CLUSTER, kafka.getSpec().getClusterCa(), ownerReference);
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt")),
                secrets());

        final X509Certificate rootCaCert0 = loadCaCert();
        final Fingerprint ca0 = PkiUtils.fingerprint(rootCaCert0);
        assertEquals(0, rootCaCert0.getBasicConstraints(), "Expect new CA cert to actually _be_ a _CA_ cert");

        // (2) Cause a EE cert to be issued
        CertificateReconcileResult crr0 = clusterCerts.reconcileCoCertificate(kafka);
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt")),
                secrets());
        assertEquals(rootCaCert0, loadCaCert(), "Don't expect the CA cert to change as a result of EE reconciliation");
        assertEquals(new CertificateReconcileResult.WaitingForTrust(rootCaCert0), crr0);

        // (3) remember some stuff in memory
        clusterCerts.recordTrustChanges();
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt")),
                secrets());
        assertEquals(TrustState.UNTRUSTED, caTrustState(ca0));
        assertEquals(CertificateState.PENDING, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());


        // (4) In real life a rolling update happens here then we infer that those certs are now trusted, in this test we just take it as read

        // (5) Use state from step (3) to update the certs we consider trusted
        clusterCerts.doTrustChanges();
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt")),
                secrets());
        assertEquals(TrustState.TRUSTED_NEW, caTrustState(ca0));
        assertEquals(CertificateState.PENDING, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());

        // (6) Now deploy the EE cert
        final CertificateReconcileResult crr1 = clusterCerts.reconcileCoCertificate(kafka);
        X509Certificate rootCaCert1 = loadCaCert();
        assertEquals(rootCaCert0, rootCaCert1);
        assertEquals(0, rootCaCert1.getBasicConstraints());
        assertEquals(new CertificateReconcileResult.OkIssued(rootCaCert1), crr1);
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.crt", "cluster-operator.key", "cluster-operator.p12", "cluster-operator.password"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt")),
                secrets());
        assertEquals(TrustState.TRUSTED_USED, caTrustState(ca0));
        assertEquals(CertificateState.IN_USE, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());
    }

    @Test
    public void testKeyReplacementByAnnotation() throws CertificateException {
        // Create an existing cluster
        clusterCreation();

        // Now reconcile again not within the renewal period with having annotated
        clock.advanceFromOrigin(Duration.of(10, ChronoUnit.DAYS));
        Secret secret = client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-ca").get();
        Secret annotatedSecret = new SecretBuilder(secret).editOrNewMetadata().addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true").endMetadata().build();
        client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-ca").replace(annotatedSecret);

        renewalReconciliation();
    }

    @Test
    public void testCertificateRenewalByAnnotation() throws CertificateException {
        // Create an existing cluster
        clusterCreation();

        // Now reconcile again not within the renewal period with having annotated
        clock.advanceFromOrigin(Duration.of(10, ChronoUnit.DAYS));
        Secret secret = client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-ca-cert").get();
        Secret annotatedSecret = new SecretBuilder(secret).editOrNewMetadata().addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true").endMetadata().build();
        client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-ca-cert").replace(annotatedSecret);

        renewalReconciliation();
    }

    // TODO actually whether this was renewal or replacement would depend on the policy
    @Test
    public void testCertificateRenewalDueToExpiry() throws CertificateException {
        // Create an existing cluster
        clusterCreation();

        // Now reconcile again within the renewal period
        clock.advanceFromOrigin(Duration.of(26, ChronoUnit.DAYS));

        renewalReconciliation();
    }

    private void renewalReconciliation() throws CertificateException {
        X509Certificate rootCaCert0 = loadCaCert();
        final Fingerprint ca0 = PkiUtils.fingerprint(rootCaCert0);

        Reconciliation reconciliation = new Reconciliation("test", "Kafka", NAMESPACE, CLUSTER);

        // (1) Bootstrap the internal CA
        ClusterPki.ClusterCerts clusterCerts = pki.bootstrapCa(reconciliation, NAMESPACE, CLUSTER, kafka.getSpec().getClusterCa(), ownerReference);

        // ^^ updates the ca.key and ca.crt only. The pki-state isn't updated yet, but will be once a cert has been issued
        // using the new CA cert
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.crt", "cluster-operator.key", "cluster-operator.p12", "cluster-operator.password"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt")
                ),
                secrets());
        X509Certificate rootCaCert1 = loadCaCert();
        final Fingerprint ca1 = PkiUtils.fingerprint(rootCaCert1);
        assertNotEquals(ca0, ca1, "Expected a _new_ CA cert");
        assertEquals(0, rootCaCert1.getBasicConstraints(), "Expect new CA cert to actually _be_ a _CA_ cert");

        // (2) Cause a EE cert to be issued
        CertificateReconcileResult crr0 = clusterCerts.reconcileCoCertificate(kafka);
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending", "cluster-operator.crt", "cluster-operator.key", "cluster-operator.p12", "cluster-operator.password"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt", ca1 + ".state", ca1 + ".crt")
                ),
                secrets());
        assertEquals(TrustState.PHASE_OUT, caTrustState(ca0), "Expected old CA cert to still be trusted");
        assertEquals(TrustState.UNTRUSTED, caTrustState(ca1), "Expect new CA cert to ne untrusted");

        assertEquals(new CertificateReconcileResult.WaitingForTrust(rootCaCert1), crr0, "Expect new CO cert issued by _new_ CA");

        // (3) remember some stuff in memory
        clusterCerts.recordTrustChanges();
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending", "cluster-operator.p12", "cluster-operator.password", "cluster-operator.crt", "cluster-operator.key"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt", ca1 + ".state", ca1 + ".crt")),
                secrets());
        assertEquals(TrustState.PHASE_OUT, caTrustState(ca0));
        assertEquals(TrustState.UNTRUSTED, caTrustState(ca1));
        assertEquals(CertificateState.PENDING, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());


        // (4) In real life a rolling update happens here then we infer that those certs are now trusted, in this test we just take it as read


        // (5) Use state from step (3) to update the certs we consider trusted
        clusterCerts.doTrustChanges();
        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.pending", "cluster-operator.p12", "cluster-operator.password", "cluster-operator.crt", "cluster-operator.key"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt", ca1 + ".state", ca1 + ".crt")),
                secrets());
        assertEquals(TrustState.PHASE_OUT, caTrustState(ca0));
        assertEquals(TrustState.TRUSTED_NEW, caTrustState(ca1));
        assertEquals(CertificateState.PENDING, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());

        // (6) Now deploy the EE cert
        final CertificateReconcileResult crr1 = clusterCerts.reconcileCoCertificate(kafka);
        assertEquals(rootCaCert1, loadCaCert());
        assertEquals(new CertificateReconcileResult.OkIssued(rootCaCert1), crr1);

        assertEquals(
                Map.of("my-cluster-cluster-ca", Set.of("ca.key"),
                        "my-cluster-cluster-ca-cert", Set.of("ca.crt"),
                        "my-cluster-cluster-operator-certs", Set.of("cluster-operator.crt", "cluster-operator.key", "cluster-operator.p12", "cluster-operator.password"),
                        "my-cluster-cluster-pki-state", Set.of(ca0 + ".state", ca0 + ".crt", ca1 + ".state", ca1 + ".crt")),
                secrets());
        assertEquals(TrustState.PHASE_OUT, caTrustState(ca0));
        assertEquals(TrustState.TRUSTED_USED, caTrustState(ca1));
        assertEquals(CertificateState.IN_USE, clusterCerts.coCertWriter.loadStatus(new ClusterOperatorIdentity()).state());

        Assumptions.assumeTrue(false, "TODO we need to implement ZOMBIE");
    }

    private X509Certificate loadCaCert() throws CertificateException {
        return PkiUtils.x509Certificate(Base64.getDecoder().decode(
                client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-ca-cert").get().getData().get("ca.crt")));
    }

    private TrustState caTrustState(Fingerprint fp) {
        return TrustController.decodeState(
                client.secrets().inNamespace(NAMESPACE).withName("my-cluster-cluster-pki-state").get().getData().get(fp + ".state"));
    }

    private Map<String, Set<String>> secrets() {
        return client.secrets().inNamespace(NAMESPACE).list().getItems().stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s.getData().keySet()));
    }

}