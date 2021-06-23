/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.File;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import io.strimzi.operator.cluster.model.pki.CertificateState;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.NewTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.PkiUtils;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TestClock;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustPrivateState;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.operator.cluster.model.pki.TrustState;
import io.strimzi.operator.cluster.model.pki.Validator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    private Resource<Secret> brokerCertsSecretResource;
    private TrustPrivateState trustPrivateState;
    private ObjectMapper mapper;
    private RequiredCertificate brokerReqCert;
    private CertificateController cc;
    private TrustController tc;
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

    @BeforeEach
    void setUp() {
        clock = new TestClock(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));
        certManager = TestOpenSslCertManager.replay("testLifecycle", new File("src/test/resources/io/strimzi/operator/cluster/model/pki/cluster"));
        //certManager = TestOpenSslCertManager.replay("testLifecycle", new File("/home/tom/messaging/operator/cluster-operator/src/test/resources/io/strimzi/operator/cluster/model/pki/cluster"));

        client = new MockKube().build();

        // TODO non-null addresses
        brokerReqCert = ClusterPki.brokerSubject(NAMESPACE, CLUSTER, 0, null, null);

        trustPrivateStateName = KafkaResources.clusterCaKeySecretName(CLUSTER);
        trustPrivateStateResource = client.secrets().inNamespace(NAMESPACE).withName(trustPrivateStateName);
        trustPrivateState = new TrustPrivateState(trustPrivateStateResource);

        trustSecretName = CLUSTER + "-cluster-ca-trust";
        trustSecretResource = client.secrets().inNamespace(NAMESPACE).withName(trustSecretName);
        NewTrustOutputWriter trustOutputWriter = new NewTrustOutputWriter(trustSecretResource);

        brokerCertsSecretName = KafkaCluster.brokersSecretName(CLUSTER);
        brokerCertsSecretResource = client.secrets().inNamespace(NAMESPACE).withName(brokerCertsSecretName);

        tc = new TrustController(trustPrivateState, List.of(trustOutputWriter));

        mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(BrokerIdentity.class, "broker"));
    }

    private void initCc(int caValidity, int certValidity, int certRenewal) throws CertificateException {
        BrokersCertificateStatusStore statusStore = new BrokersCertificateStatusStore(mapper, null, brokerCertsSecretResource);
        NewBrokersCertificateOutputWriter outputStore = new NewBrokersCertificateOutputWriter(brokerCertsSecretResource);

//        InternalCertificateIssuer issuer = InternalCertificateIssuer.createRootIssuer(clock,
//                new OpenSslCertManager(clock), new Subject.Builder().withCommonName("My CA").build(), caValidity);

        Chain chain = new Chain(clock, client.secrets(), certManager);
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
        CertificateIssuer issuer = chain.reconcile(reconciliation, internalIssuerSpec, null);
        g++;
        cc = new CertificateController(
                trustPrivateState, statusStore,
                List.of(outputStore),
                issuer, new Validator(clock, certRenewal), certValidity);
    }

    @AfterEach
    void tearDown() {
    }

    @SuppressWarnings({"checkstyle:LocalVariableName", "checkstyle:MethodLength"})
    @Test
    public void testLifecycle() throws CertificateException, JsonProcessingException {
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
        CertificateReconcileResult crr1 = cc.reconcile(brokerReqCert);
        Assertions.assertEquals(CertificateReconcileResult.State.WAITING_FOR_TRUST, crr1.state());
        assertNotNull(crr1.trustRoot(), "Expected a trust root");
        assertEquals(rootFingerprint0, ((Object) crr1.trustRoot().fingerprint()).toString());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state")),
                secretsKeySets(),
                "Expected new Secrets " + trustSecretName + " and " + brokerCertsSecretName);
        // And broker secret should show pending state
        assertCertificateStatus("broker-0.state", CertificateState.PENDING, clientFingerprint0_0, rootFingerprint0);
        assertEquals(Map.of(rootFingerprint0, TrustState.UNTRUSTED), trustPrivateState());
        TrustRoot trustRoot = crr1.trustRoot();

        // Reconcile the trust controller, to establish trust in the CA certificate
        reconcileTrustToTrusted(trustRoot);
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected new Secret " + trustSecretName);
        Map<String, String> trustCerts = trustSecretResource.get().getData();

        // Now reconcile the certificate again, because the CA cert is now trusted it should transition to in use

        // TODO this doesn't prove it's a no-op (e.g. it's the same as reconcileCertToInUse). We should do something to prove that.
        CertificateReconcileResult crr2 = cc.reconcile(brokerReqCert);
        Assertions.assertEquals(CertificateReconcileResult.State.OK_ISSUED, crr2.state());
        assertNull(crr2.trustRoot(), "Did not expect a trust root");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());
        assertCertificateStatus("broker-0.state", CertificateState.IN_USE,
                clientFingerprint0_0,
                rootFingerprint0);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected additional items in " + brokerCertsSecretName);
        Map<String, String> initialBrokerCerts = brokerCertsSecretResource.get().getData();

        // reconcile the trust controller again, should be a no op
        reconcileTrustNoop(trustRoot);
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());
        assertEquals(trustCerts, trustSecretResource.get().getData());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");

        // reconcile the certificate again, should be a no op

        // TODO this doesn't prove it's a no-op (e.g. it's the same as reconcileCertToInUse). We should do something to prove that.
        CertificateReconcileResult crr3 = cc.reconcile(brokerReqCert);
        // And we have a trust relationship
        Assertions.assertEquals(CertificateReconcileResult.State.OK_NOOP, crr3.state());
        assertNull(crr3.trustRoot(), "Did not expect a trust root");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());
        assertCertificateStatus("broker-0.state", CertificateState.IN_USE,
                clientFingerprint0_0,
                rootFingerprint0);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(initialBrokerCerts, brokerCertsSecretResource.get().getData(),
                "Expected broker certs to be unchanged");

        // advance the time and reconcile the certificate again, trust relationship won't change, but expect OK_ISSUED
        clock.advanceFromOrigin(Duration.ofDays(certValidity - certRenewal + 1));
        // The CA cert is still valid, so the trust relationship shouldn't change
        reconcileTrustNoop(trustRoot);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());

        // TODO this doesn't prove it's a no-op (e.g. it's the same as reconcileCertToInUse). We should do something to prove that.
        CertificateReconcileResult crr4 = cc.reconcile(brokerReqCert);
        // And we have a trust relationship
        Assertions.assertEquals(CertificateReconcileResult.State.OK_ISSUED, crr4.state());
        assertNull(crr4.trustRoot(), "Did not expect a trust root");

        assertNull(crr4.trustRoot());
        assertEquals(Map.of(((Object) trustRoot.fingerprint()).toString(), TrustState.TRUSTED), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertCertificateStatus("broker-0.state", CertificateState.IN_USE,
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
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(rootFingerprint1, secretItemFingerprint(chainCertSecretName, chainCertItem));

        reconcileTrustNoop(trustRoot);
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected the secret keysets to be the same");
        assertEquals(Map.of(rootFingerprint0, TrustState.TRUSTED), trustPrivateState());

        CertificateReconcileResult crr5 = cc.reconcile(brokerReqCert);
        // We should be waiting for a trust relationship
        Assertions.assertEquals(CertificateReconcileResult.State.WAITING_FOR_TRUST, crr5.state());
        assertNotNull(crr5.trustRoot(), "Expected a trust root");
        assertEquals(Map.of(rootFingerprint1, TrustState.UNTRUSTED,
                rootFingerprint0, TrustState.TRUSTED), trustPrivateState());
        assertEquals(
                Map.of(chainKeySecretName, Set.of(chainKeyItem),
                        chainCertSecretName, Set.of(chainCertItem),
                        trustPrivateStateName, Set.of(rootFingerprint0 + ".crt", rootFingerprint1 + ".crt"),
                        brokerCertsSecretName, Set.of("broker-0.state", "broker-0.p12", "broker-0.password"),
                        trustSecretName, Set.of(rootFingerprint0 + ".crt")),
                secretsKeySets(),
                "Expected additional item " + rootFingerprint1 + ".crt in " + trustPrivateStateName);
        assertCertificateStatus("broker-0.state", CertificateState.PENDING, clientFingerprint1_0, rootFingerprint1);
        assertEquals(Map.of(
                rootFingerprint0, TrustState.TRUSTED,
                rootFingerprint1, TrustState.UNTRUSTED), trustPrivateState());
        assertNotEquals(initialBrokerCerts, brokerCertsSecretResource.get().getData());
        initialBrokerCerts = brokerCertsSecretResource.get().getData();
        assertEquals(trustCerts, trustSecretResource.get().getData());

        reconcileTrustToTrusted(crr5.trustRoot());
        assertEquals(Map.of(rootFingerprint1, TrustState.TRUSTED), trustPrivateState());

    }

    private Map<String, TrustState> trustPrivateState() {
        return ((Map<? extends Object, TrustRoot>) trustPrivateState.load()).entrySet()
                .stream()
                .collect(Collectors.toMap(
                    entry -> entry.getKey().toString(),
                    entry -> entry.getValue().state()));
    }

    private String secretItemFingerprint(String secret, String item) throws CertificateException {
        return ((Object) PkiUtils.fingerprint(PkiUtils.x509Certificate(Base64.getDecoder().decode(client.secrets()
                .withName(secret).get().getData().get(item))))).toString();
    }

    private Map<String, Set<String>> secretsKeySets() {
        return client.secrets().list().getItems().stream().collect(Collectors.toMap(
            secret -> secret.getMetadata().getName(),
            secret -> secret.getData().keySet()));
    }

    // TODO also need a test for where the CertificateIssuer doesn't immediately issue the certificate

    private void reconcileTrustToTrusted(TrustRoot trustRoot) throws CertificateEncodingException {
        Object fingerprint = trustRoot.fingerprint();
        // We can reconcile the trust relationship
        boolean rollBroker = tc.reconcile(Set.of(trustRoot.toTrusted("123")));
        assertTrue(rollBroker, "Expected TrustController to cause roll");
        Secret secret = trustPrivateStateResource.get();
        assertNotNull(secret);
        Map<String, String> data = secret.getData();
        assertNotNull(data);
        assertEquals(
                Set.of(fingerprint + ".crt"),
                data.keySet());
        String c = data.get(fingerprint + ".crt");
        assertTrue(c != null && c.length() > 100);
        Map<String, String> stringData = secret.getStringData();
        assertNotNull(stringData);
        assertEquals(
                Set.of(fingerprint + ".state"),
                stringData.keySet());
        String s = stringData.get(fingerprint + ".state");
        assertEquals("trusted", s);
        assertEquals(Set.of(fingerprint + ".crt"), trustPrivateStateResource.get().getData().keySet());

    }

    private void reconcileTrustNoop(TrustRoot trustRoot) throws CertificateEncodingException {
        Object fingerprint = trustRoot.fingerprint();
        // We can reconcile the trust relationship
        boolean rollBroker = tc.reconcile(Set.of(trustRoot.toTrusted("123")));
        assertFalse(rollBroker, "Expected TrustController to cause not roll");
        Secret secret = trustPrivateStateResource.get();
        assertNotNull(secret);
        Map<String, String> data = secret.getData();
        assertNotNull(data);
        assertEquals(
                Set.of(fingerprint + ".crt"),
                data.keySet());
        String c = data.get(fingerprint + ".crt");
        assertTrue(c != null && c.length() > 100);
        Map<String, String> stringData = secret.getStringData();
        assertNotNull(stringData);
        assertEquals(
                Set.of(fingerprint + ".state"),
                stringData.keySet());
        String s = stringData.get(fingerprint + ".state");
        assertEquals("trusted", s);
        assertEquals(Set.of(fingerprint + ".crt"), trustPrivateStateResource.get().getData().keySet());
    }

    private void assertCertificateStatus(String secretItem, CertificateState state, String expectedIssuedFingerprint, String expectedIssuerFingerprint) throws JsonProcessingException {
        Secret brokerSecret = brokerCertsSecretResource.get();
        assertNotNull(brokerSecret);
        assertNotNull(brokerSecret.getData());

        CertificateStatus certificateStatus = mapper.readValue(brokerSecret.getData().get(secretItem), CertificateStatus.class);
        Assertions.assertEquals(state, certificateStatus.state());
        assertEquals(new BrokerIdentity(0), certificateStatus.subjectIdentity());
        assertNull(certificateStatus.tokenBytes());
        assertNotNull(certificateStatus.issuedCertificate());
        assertEquals(expectedIssuedFingerprint, ((Object) PkiUtils.fingerprint(certificateStatus.issuedCertificate().issuedCertificate())).toString());
        assertEquals(expectedIssuerFingerprint, ((Object) PkiUtils.fingerprint(certificateStatus.issuedCertificate().rootCaCertificate())).toString());
    }

}