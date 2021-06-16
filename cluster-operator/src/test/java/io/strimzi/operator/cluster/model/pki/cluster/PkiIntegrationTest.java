/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.pki.CertificateController;
import io.strimzi.operator.cluster.model.pki.CertificateIssuer;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.CertificateState;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.NewTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TestClock;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustPrivateState;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.operator.cluster.model.pki.TrustState;
import io.strimzi.operator.cluster.model.pki.Validator;
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

    @BeforeEach
    void setUp() {
        clock = new TestClock(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));
        client = new MockKube().build();

        // TODO non-null addresses
        brokerReqCert = ClusterPki.brokerSubject(NAMESPACE, CLUSTER, 0, null, null);

        trustPrivateStateResource = client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER));
        trustSecretResource = client.secrets().inNamespace(NAMESPACE).withName(CLUSTER + "-cluster-ca-trust");
        brokerCertsSecretResource = client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER));
        trustPrivateState = new TrustPrivateState(trustPrivateStateResource);
        NewTrustOutputWriter trustOutputWriter = new NewTrustOutputWriter(trustSecretResource);
        mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(BrokerIdentity.class, "broker"));
        tc = new TrustController(trustPrivateState, List.of(trustOutputWriter));
    }

    int g = 1;

    private void initCc(int caValidity, int certValidity, int certRenewal) throws CertificateException {
        BrokersCertificateStatusStore statusStore = new BrokersCertificateStatusStore(mapper, brokerCertsSecretResource);
        NewBrokersCertificateOutputWriter outputStore = new NewBrokersCertificateOutputWriter(brokerCertsSecretResource);

//        InternalCertificateIssuer issuer = InternalCertificateIssuer.createRootIssuer(clock,
//                new OpenSslCertManager(clock), new Subject.Builder().withCommonName("My CA").build(), caValidity);
        Chain chain = new Chain(clock, client.secrets(), new OpenSslCertManager(clock));
        internalIssuerSpec = new Chain.InternalIssuerSpec(certRenewal,
                List.of(
                        new Chain.CaCertificateReference("ggg", "ca.crt", "hhh", "ca.key",
                                new Chain.IssuanceParams(new Subject.Builder().withCommonName("my CA " + g).build(),
                                        caValidity, CertificateExpirationPolicy.REPLACE_KEY))
                )
        );
        CertificateIssuer issuer = chain.reconcile(internalIssuerSpec, null);
        g++;
        cc = new CertificateController(
                trustPrivateState, statusStore,
                List.of(outputStore),
                issuer, new Validator(clock, certRenewal), certValidity);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    public void testLifecycle() throws CertificateException, JsonProcessingException {
        int caValidity = 30;
        int certValidity = 15;
        int certRenewal = 5;
        initCc(caValidity, certValidity, certRenewal);

        // Initial certificate reconciliation, which will have a new CA certificate which won't be trusted yet
        TrustRoot trustRoot = reconcileCertToPending(Set.of("broker-0.state")).trustRoot();
        assertNull(trustSecretResource.get());
        Object fingerprint = trustRoot.fingerprint();
        assertEquals(Set.of(fingerprint + ".crt"), trustPrivateStateResource.get().getData().keySet());
        assertEquals(Set.of(fingerprint), trustPrivateState.load().keySet());

        // Reconcile the trust controller, to establish trust in the CA certificate
        reconcileTrustToTrusted(trustRoot, fingerprint);
        Map<String, String> trustCerts = trustSecretResource.get().getData();

        // Now reconcile the certificate again, because the CA cert is now trusted it should transition to in use
        reconcileCertOk(fingerprint, CertificateReconcileResult.State.OK_ISSUED);

        Map<String, String> brokerCerts = brokerCertsSecretResource.get().getData();

        // reconcile the trust controller again, should be a no op
        reconcileTrustNoop(trustRoot, fingerprint);
        assertEquals(trustCerts, trustSecretResource.get().getData());

        // reconcile the certificate again, should be a no op
        reconcileCertOk(fingerprint, CertificateReconcileResult.State.OK_NOOP);
        assertEquals(brokerCerts, brokerCertsSecretResource.get().getData());

        // advance the time and reconcile the certificate again, trust relationship won't change, but expect OK_ISSUED
        clock.advanceFromOrigin(Duration.ofDays(certValidity - certRenewal + 1));
        // The CA cert is still valid, so the trust relationship shouldn't change
        reconcileTrustNoop(trustRoot, fingerprint);
        assertNull(reconcileCertOk(fingerprint, CertificateReconcileResult.State.OK_ISSUED).trustRoot());
        assertNotEquals(brokerCerts, brokerCertsSecretResource.get().getData());
        brokerCerts = brokerCertsSecretResource.get().getData();
        assertEquals(trustCerts, trustSecretResource.get().getData());

        // advance the clock so that the CA cert will expire and check that the TrustState changes appropriately
        clock.advanceFromOrigin(Duration.ofDays(28));
        initCc(caValidity, certValidity, certRenewal);
        reconcileTrustNoop(trustRoot, fingerprint);
        TrustRoot trustRoot2 = reconcileCertToPending(Set.of("broker-0.state", "broker-0.p12", "broker-0.password")).trustRoot();
        Object fingerprint2 = trustRoot2.fingerprint();
        assertEquals(Set.of(fingerprint + ".crt", fingerprint2 + ".crt"), trustPrivateStateResource.get().getData().keySet());
        assertEquals(Set.of(fingerprint, fingerprint2), trustPrivateState.load().keySet());
        assertNotEquals(brokerCerts, brokerCertsSecretResource.get().getData());
        brokerCerts = brokerCertsSecretResource.get().getData();
        assertEquals(trustCerts, trustSecretResource.get().getData());

        reconcileTrustToTrusted(trustRoot2, fingerprint2);

    }

    // TODO also need a test for where the CertificateIssuer doesn't immediately issue the certificate

    private void reconcileTrustToTrusted(TrustRoot trustRoot, Object fingerprint) throws CertificateEncodingException {
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
        assertEquals(Set.of(fingerprint), trustPrivateState.load().keySet());
        assertEquals(TrustState.TRUSTED, trustPrivateState.load().get(fingerprint).state());
    }

    private void reconcileTrustNoop(TrustRoot trustRoot, Object fingerprint) throws CertificateEncodingException {
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
        assertEquals(Set.of(fingerprint), trustPrivateState.load().keySet());
        assertEquals(TrustState.TRUSTED, trustPrivateState.load().get(fingerprint).state());
    }

    private CertificateReconcileResult reconcileCertOk(Object fingerprint,
                                                       CertificateReconcileResult.State expectedState) throws CertificateException, JsonProcessingException {

        // TODO this doesn't prove it's a no-op (e.g. it's the same as reconcileCertToInUse). We should do something to prove that.
        CertificateReconcileResult crr = cc.reconcile(brokerReqCert);
        // And we have a trust relationship
        Assertions.assertEquals(expectedState, crr.state());
        assertNull(crr.trustRoot());

        assertEquals(Set.of(fingerprint + ".crt"), trustSecretResource.get().getData().keySet());
        assertEquals(Set.of(fingerprint), trustPrivateState.load().keySet());
        assertEquals(TrustState.TRUSTED, trustPrivateState.load().get(fingerprint).state());
        assertNotNull(trustSecretResource.get());
        // Broker secret shows is-use state
        Secret brokerSecret = brokerCertsSecretResource.get();
        assertNotNull(brokerSecret);
        assertNotNull(brokerSecret.getData());
        assertEquals(Set.of("broker-0.state", "broker-0.p12", "broker-0.password"), brokerSecret.getData().keySet());
        CertificateStatus certificateStatus = mapper.readValue(brokerSecret.getData().get("broker-0.state"), CertificateStatus.class);
        Assertions.assertEquals(CertificateState.IN_USE, certificateStatus.state());
        assertEquals(new BrokerIdentity(0), certificateStatus.subjectIdentity());
        assertNull(certificateStatus.tokenBytes());
        assertNotNull(certificateStatus.issuedCertificate());

        return crr;
    }

    private CertificateReconcileResult reconcileCertToPending(Set<String> expectedBrokerSecret) throws CertificateException, JsonProcessingException {
        CertificateReconcileResult crr = cc.reconcile(brokerReqCert);

        // We should be waiting for a trust relationship
        Assertions.assertEquals(CertificateReconcileResult.State.WAITING_FOR_TRUST, crr.state());
        assertNotNull(crr.trustRoot());
        Object fingerprint = crr.trustRoot().fingerprint();
        assertEquals(TrustState.UNTRUSTED, trustPrivateState.load().get(fingerprint).state());

        // And broker secret should show pending state
        Secret brokerSecret = brokerCertsSecretResource.get();
        assertNotNull(brokerSecret);
        assertNotNull(brokerSecret.getData());
        assertEquals(expectedBrokerSecret, brokerSecret.getData().keySet());
        CertificateStatus certificateStatus = mapper.readValue(brokerSecret.getData().get("broker-0.state"), CertificateStatus.class);
        Assertions.assertEquals(CertificateState.PENDING, certificateStatus.state());
        assertEquals(new BrokerIdentity(0), certificateStatus.subjectIdentity());
        assertNull(certificateStatus.tokenBytes());
        assertNotNull(certificateStatus.issuedCertificate());

        return crr;
    }

}