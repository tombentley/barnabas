/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.pki.CertificateController;
import io.strimzi.operator.cluster.model.pki.CertificateIssuer;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkIssued;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkNoop;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.WaitingForTrust;
import io.strimzi.operator.cluster.model.pki.CertificateState;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.Fingerprint;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.NewTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.PkiUtils;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TestClock;
import io.strimzi.operator.cluster.model.pki.TrustController2;
import io.strimzi.operator.cluster.model.pki.TrustManager;
import io.strimzi.operator.cluster.model.pki.TrustState;
import io.strimzi.operator.cluster.model.pki.UseManager;
import io.strimzi.operator.cluster.model.pki.Validator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test is a simulation of how the KAO would use the TC and CC to build the cluster PKI.
 */
class PkiIntegrationTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String CLUSTER = "my-cluster";

    KubernetesClient client;
    private TestClock clock;
    private Resource<Secret> trustPrivateStateResource;
    private Resource<Secret> trustSecretResource;
    private NamedResource<Secret> brokerCertsSecretResource;
    private UseManager useManager;
    private TrustManager trustManager;
    private ObjectMapper mapper;
    private RequiredCertificate brokerReqCert;
    private CertificateController cc;
    private Chain.InternalIssuerSpec internalIssuerSpec;
    private CertManager certManager;
    int g = 1; // TODO get rid of this!
    private String chainCertSecretName;
    private String chainKeySecretName;
    private String chainCertItem;
    private String chainKeyItem;
    private String trustPrivateStateName;
    private String trustSecretName;
    private String brokerCertsSecretName;
    private Reconciliation reconciliation = new Reconciliation("test", "Kafka", "ns", "test");
    private BrokersCertificateOutputWriter outputStore;

    @BeforeEach
    void setUp() {
        clock = new TestClock(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));
        certManager = TestOpenSslCertManager.replay("testLifecycle", new File("src/test/resources/io/strimzi/operator/cluster/model/pki/cluster"));
        //certManager = TestOpenSslCertManager.replay("testLifecycle", new File("/home/tom/messaging/operator/cluster-operator/src/test/resources/io/strimzi/operator/cluster/model/pki/cluster"));

        client = new MockKube().build();

        // TODO non-null addresses
        brokerReqCert = ClusterPki.brokerSubject(NAMESPACE, CLUSTER, 0, null, null);


        var trustPrivateStateOp = client.secrets().inNamespace(NAMESPACE);
        trustPrivateStateName = KafkaResources.clusterCaKeySecretName(CLUSTER);
        TrustController2 tc = new TrustController2(reconciliation, new NamedResource<>(trustPrivateStateOp, trustPrivateStateName));
        this.useManager = tc;
        this.trustManager = tc;
        trustPrivateStateResource = trustPrivateStateOp.withName(trustPrivateStateName);

        trustSecretName = CLUSTER + "-cluster-ca-trust";
        trustSecretResource = trustPrivateStateOp.withName(trustSecretName);
        NewTrustOutputWriter trustOutputWriter = new NewTrustOutputWriter(trustSecretResource);

        brokerCertsSecretName = KafkaCluster.brokersSecretName(CLUSTER);
        brokerCertsSecretResource = new NamedResource<>(trustPrivateStateOp, brokerCertsSecretName);

        mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(BrokerIdentity.class, "kafka"));
    }

    private void initCc(int caValidity, int certValidity, int certRenewal) throws CertificateException {

        outputStore = new BrokersCertificateOutputWriter(
                reconciliation,
                CLUSTER,
                new NamedResource<>(client.secrets().inNamespace(NAMESPACE), brokerCertsSecretName),
                new NamedResource<>(client.secrets().inNamespace(NAMESPACE), brokerCertsSecretName),
                null,
                true, true);

//        InternalCertificateIssuer issuer = InternalCertificateIssuer.createRootIssuer(clock,
//                new OpenSslCertManager(clock), new Subject.Builder().withCommonName("My CA").build(), caValidity);

        Chain chain = new Chain(reconciliation, clock, client.secrets(), certManager);
        chainCertSecretName = "ggg";
        chainKeySecretName = "hhh";
        chainCertItem = "ca.crt";
        chainKeyItem = "ca.key";
        internalIssuerSpec = new Chain.InternalIssuerSpec(certRenewal,
                List.of(
                        new Chain.CaCertificateReference(chainCertSecretName, chainCertItem, chainKeySecretName, chainKeyItem,
                                new Chain.IssuanceParams(new Subject.Builder().withCommonName("my CA " + g).build(),
                                        caValidity, CertificateExpirationPolicy.REPLACE_KEY))
                )
        );
        CertificateIssuer issuer = chain.reconcile(internalIssuerSpec, null);
        g++;
        cc = new CertificateController(reconciliation,
                useManager,
                issuer,
                new Validator(clock, certRenewal),
                certValidity);
    }

    @AfterEach
    void tearDown() {
    }

    @SuppressWarnings({"checkstyle:LocalVariableName", "checkstyle:MethodLength"})
    @Test
    public void testLifecycle() throws CertificateException, IOException {
        int caValidity = 30;
        int certValidity = 15;
        int certRenewal = 5;
        String rootFingerprint0 = "2D507CF7021DA69B48DD71D19CDAD257C23CD951";
        String clientFingerprint0_0 = "ECCF853623216F9AE6FC68B5CB34942626E5D88C";
        String clientFingerprint0_1 = "D73FA81BCF62EEDE9D769831C40B02C4FB65AA0E"; // when client cert expires

        assertEquals(Map.of(), secretsKeySets());

        // Reconcile the Chain, creating secrets with CA cert & key
        initCc(caValidity, certValidity, certRenewal);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem)),
                secretsKeySets(),
                "Expected " + chainKeySecretName + " and " + chainCertSecretName + " to be created");
        assertEquals(rootFingerprint0, secretItemFingerprint(chainCertSecretName, chainCertItem));

        // Initial certificate reconciliation, resulting in a new CA certificate which won't be trusted yet
        CertificateReconcileResult crr1 = cc.reconcile(brokerReqCert, outputStore);
        Assertions.assertEquals(WaitingForTrust.class, crr1.getClass());
        X509Certificate trustRoot = crr1.trustRoot().orElseThrow();
        assertEquals(rootFingerprint0, PkiUtils.fingerprint(trustRoot).toString());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state")),
                secretsKeySets(),
                "Expected new Secrets " + trustSecretName + " and " + brokerCertsSecretName);
        // And broker secret should show pending state
        assertCertificateStatus("my-cluster-kafka-0.state", CertificateState.PENDING, clientFingerprint0_0, rootFingerprint0);
        assertEquals(Map.of(rootFingerprint0, TrustState.UNTRUSTED), trustPrivateState());

        // Reconcile the trust controller, to establish trust in the CA certificate
        reconcileTrustExpectingTrusted(trustRoot);
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected new Secret " + trustSecretName);
        Map<String, String> trustCerts = trustSecretResource.get().getData();

        // Now reconcile the certificate again, because the CA cert is now trusted it should transition to in use
        CertificateReconcileResult crr2 = cc.reconcile(brokerReqCert, outputStore);
        Assertions.assertEquals(OkIssued.class, crr2.getClass());
        assertEquals(rootFingerprint0, PkiUtils.fingerprint(crr2.trustRoot().orElseThrow()).toString(), "Expected the root CA as trust root");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());
        assertCertificateStatus("my-cluster-kafka-0.state", CertificateState.IN_USE,
                clientFingerprint0_0,
                rootFingerprint0);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected additional items in " + brokerCertsSecretName);
        Map<String, String> initialBrokerCerts = brokerCertsSecretResource.get().getData();

        // reconcile the trust controller again, should be a no op
        reconcileTrustExpectingNoop(trustRoot);
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());
        assertEquals(trustCerts, trustSecretResource.get().getData());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");

        // reconcile the certificate again, should be a no op
        CertificateReconcileResult crr3 = cc.reconcile(brokerReqCert, outputStore);
        // And we have a trust relationship
        Assertions.assertEquals(OkNoop.class, crr3.getClass());
        assertEquals(rootFingerprint0, PkiUtils.fingerprint(crr3.trustRoot().orElseThrow()).toString(), "Expected the root CA as trust root");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());
        assertCertificateStatus("my-cluster-kafka-0.state", CertificateState.IN_USE,
                clientFingerprint0_0,
                rootFingerprint0);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(initialBrokerCerts, brokerCertsSecretResource.get().getData(),
                "Expected broker certs to be unchanged");

        // advance the time and reconcile the certificate again, trust relationship won't change, but expect OK_ISSUED
        clock.advanceFromOrigin(Duration.ofDays(certValidity - certRenewal + 1));
        // The CA cert is still valid, so the trust relationship shouldn't change
        reconcileTrustExpectingNoop(trustRoot);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());

        CertificateReconcileResult crr4 = cc.reconcile(brokerReqCert, outputStore);
        // And we have a trust relationship
        Assertions.assertEquals(CertificateReconcileResult.OkIssued.class, crr4.getClass());
        assertEquals(rootFingerprint0, PkiUtils.fingerprint(crr4.trustRoot().orElseThrow()).toString(), "Expected the root CA as trust root");
        assertEquals(Map.of(PkiUtils.fingerprint(trustRoot).toString(), TrustState.TRUSTED_NEW), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertCertificateStatus("my-cluster-kafka-0.state", CertificateState.IN_USE,
                clientFingerprint0_1,
                rootFingerprint0);
        assertNotEquals(initialBrokerCerts, brokerCertsSecretResource.get().getData());
        initialBrokerCerts = brokerCertsSecretResource.get().getData();
        assertEquals(trustCerts, trustSecretResource.get().getData());

        // advance the clock so that the CA cert will expire and check that the TrustState changes appropriately
        String rootFingerprint1 = "294E0E39228CA56A37A134A16CBFCE49FE69584A";
        String clientFingerprint1_0 = "84D2A876ACF24ABF9E8FC515B2D1C6D26912631D"; // when root CA expiry forces a 2nd client cert issuance
        clock.advanceFromOrigin(Duration.ofDays(28));

        initCc(caValidity, certValidity, certRenewal);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(rootFingerprint1, secretItemFingerprint(chainCertSecretName, chainCertItem));

        reconcileTrustExpectingNoop(trustRoot);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());

        CertificateReconcileResult crr5 = cc.reconcile(brokerReqCert, outputStore);
        // We should be waiting for a trust relationship
        Assertions.assertEquals(WaitingForTrust.class, crr5.getClass());
        assertNotNull(((WaitingForTrust) crr5).trustRoot(), "Expected a trust root");
        assertEquals(Map.of(rootFingerprint1, TrustState.UNTRUSTED,
                rootFingerprint0, TrustState.TRUSTED_NEW), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint0 + ".state",
                                rootFingerprint1 + ".crt", rootFingerprint1 + ".state"),
                        brokerCertsSecretName, Set.of("my-cluster-kafka-0.state", "my-cluster-kafka-0.p12",
                                "my-cluster-kafka-0.password", "my-cluster-kafka-0.crt", "my-cluster-kafka-0.key"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected additional item " + rootFingerprint1 + ".crt in " + trustPrivateStateName);
        assertCertificateStatus("my-cluster-kafka-0.state", CertificateState.PENDING, clientFingerprint1_0, rootFingerprint1);
        assertEquals(Map.of(
                rootFingerprint0, TrustState.TRUSTED_NEW,
                rootFingerprint1, TrustState.UNTRUSTED), trustPrivateState());
        assertNotEquals(initialBrokerCerts, brokerCertsSecretResource.get().getData());
        initialBrokerCerts = brokerCertsSecretResource.get().getData();
        assertEquals(trustCerts, trustSecretResource.get().getData());

        reconcileTrustExpectingTrusted(crr5.trustRoot().orElseThrow());
        assertEquals(Map.of(rootFingerprint1, TrustState.TRUSTED_NEW), trustPrivateState());

    }

    private Map<String, TrustState> trustPrivateState() {
        Secret secret = trustPrivateStateResource.get();
        if (secret == null || secret.getData() == null
                || secret.getData().isEmpty()) {
            return Map.of();
        } else {
            return secret.getData().entrySet().stream()
                    .filter(entry -> TrustController2.isStateItem(entry.getKey()))
                    .collect(Collectors.toMap(
                            entry -> TrustController2.fingerprint(entry.getKey()),
                            entry -> TrustController2.decodeState(entry.getValue())));
        }
    }

    private String secretItemFingerprint(String secret, String item) throws CertificateException {
        return PkiUtils.fingerprint(PkiUtils.x509Certificate(Base64.getDecoder().decode(client.secrets()
                .withName(secret).get().getData().get(item)))).toString();
    }

    private Map<String, Set<String>> secretsKeySets() {
        return client.secrets().list().getItems().stream().collect(Collectors.toMap(
            secret -> secret.getMetadata().getName(),
            secret -> secret.getData().keySet()));
    }

    // TODO also need a test for where the CertificateIssuer doesn't immediately issue the certificate

    private void reconcileTrustExpectingTrusted(X509Certificate trustRoot) throws CertificateEncodingException {
        Fingerprint fingerprint = PkiUtils.fingerprint(trustRoot);
        // We can reconcile the trust relationship
        trustManager.haveTrust(Set.of(trustRoot));
        Secret secret = trustPrivateStateResource.get();
        assertNotNull(secret);
        Map<String, String> data = secret.getData();
        assertNotNull(data);
        assertEquals(
                Set.of(fingerprint + ".crt", fingerprint + ".state"),
                data.keySet());
        String c = data.get(fingerprint + ".crt");
        assertTrue(c != null && c.length() > 100);
        String s = data.get(fingerprint + ".state");
        assertEquals("trusted_new", new String(Base64.getDecoder().decode(s), StandardCharsets.US_ASCII));
        assertEquals(Set.of(fingerprint + ".crt", fingerprint + ".state"), trustPrivateStateResource.get().getData().keySet());

    }

    private void reconcileTrustExpectingNoop(X509Certificate trustRoot) throws CertificateEncodingException {
        Fingerprint fingerprint = PkiUtils.fingerprint(trustRoot);
        // We can reconcile the trust relationship
        trustManager.haveTrust(Set.of(trustRoot));
        Secret secret = trustPrivateStateResource.get();
        assertNotNull(secret);
        Map<String, String> data = secret.getData();
        assertNotNull(data);
        assertEquals(
                Set.of(fingerprint + ".crt", fingerprint + ".state"),
                data.keySet());
        String c = data.get(fingerprint + ".crt");
        assertTrue(c != null && c.length() > 100);
        String s = data.get(fingerprint + ".state");
        assertEquals("trusted", new String(Base64.getDecoder().decode(s), StandardCharsets.US_ASCII));
        assertEquals(Set.of(fingerprint + ".crt", fingerprint + ".state"), trustPrivateStateResource.get().getData().keySet());
    }

    private void assertCertificateStatus(String secretItem, CertificateState state, String expectedIssuedFingerprint, String expectedIssuerFingerprint) throws IOException {
        Secret brokerSecret = brokerCertsSecretResource.get();
        assertNotNull(brokerSecret);
        assertNotNull(brokerSecret.getData());

        CertificateStatus certificateStatus = mapper.readValue(
                Base64.getDecoder().decode(brokerSecret.getData().get(secretItem)), CertificateStatus.class);
        Assertions.assertEquals(state, certificateStatus.state());
        assertEquals(new BrokerIdentity(0), certificateStatus.subjectIdentity());
        assertNull(certificateStatus.tokenBytes());
        assertNotNull(certificateStatus.issuedCertificate());
        assertEquals(expectedIssuedFingerprint, certificateStatus.issuedCertificate().issuedFingerprint().toString());
        assertEquals(expectedIssuerFingerprint, certificateStatus.issuedCertificate().rootCaFingerprint().toString());
    }

}