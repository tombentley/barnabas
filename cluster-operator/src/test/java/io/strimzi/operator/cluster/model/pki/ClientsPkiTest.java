/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test is a simulation of how the KAO would use the TC and CC to build the cluster PKI.
 * TODO we also need a ClientsPkiTest test
 */
class ClientsPkiTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String CLUSTER = "my-cluster";

    KubernetesClient client;
    private Clock clock = Clock.systemUTC();

    @BeforeEach
    void setUp() {
        client = new MockKube().build();
    }

    @AfterEach
    void tearDown() {
    }

    public static RequiredCertificate userCert(String userName) {
        Subject.Builder subject = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(userName);
        return new RequiredCertificate(new KafkaUserIdentity(userName), subject.build());
    }

    @Test
    public void test1() throws CertificateException, JsonProcessingException {
        Resource<Secret> trustPrivateStateResource = client.secrets().inNamespace(NAMESPACE).withName(KafkaResources.clusterCaKeySecretName(CLUSTER));
        var trustSecretResource = client.secrets().inNamespace(NAMESPACE).withName(CLUSTER + "-clients-ca-trust");
        var brokerCertsSecretResource = client.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER));
        TrustPrivateState trustPrivateState = new TrustPrivateState(trustPrivateStateResource);
        NewTrustOutputWriter trustOutputWriter = new NewTrustOutputWriter(trustSecretResource);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(KafkaUserIdentity.class, "user"));
        BrokersCertificateStatusStore statusStore = new BrokersCertificateStatusStore(mapper, brokerCertsSecretResource);
        NewBrokersCertificateOutputWriter outputStore = new NewBrokersCertificateOutputWriter(brokerCertsSecretResource);

        TrustController tc = new TrustController(trustPrivateState, List.of(trustOutputWriter));

        StrimziCertificateIssuer issuer = StrimziCertificateIssuer.createRootIssuer(clock,
                new OpenSslCertManager(), new Subject.Builder().withCommonName("My Clients CA").build(), 10);

        RequiredCertificate brokerReqCert = userCert("cn=MyUser");

        CertificateController cc = new CertificateController(clock,
                trustPrivateState, statusStore,
                List.of(outputStore),
                issuer, 15);

        // UO will do Initial certificate reconciliation
        var crr = cc.reconcile(new RequiredCertificate(new BrokerIdentity(0), brokerReqCert.subjectAndSan()));
        // assertNotNull(trustPrivateStateResource.get()); // TODO this should be UNTRUSTED
        assertNull(trustSecretResource.get());
        // Broker secret shows pending state
        Secret brokerSecret = brokerCertsSecretResource.get();
        assertNotNull(brokerSecret);
        assertNotNull(brokerSecret.getData());
        assertEquals(Set.of("broker-0.state"), brokerSecret.getData().keySet());
        CertificateStatus certificateStatus = mapper.readValue(brokerSecret.getData().get("broker-0.state"), CertificateStatus.class);
        assertEquals(CertificateState.PENDING, certificateStatus.state());
        assertEquals(new BrokerIdentity(0), certificateStatus.subjectIdentity());
        assertNull(certificateStatus.tokenBytes());
        assertNotNull(certificateStatus.issuedCertificate());
        // And we're waiting for a trust relationship
        assertEquals(CertificateReconcileResult.State.WAITING_FOR_TRUST, crr.state());
        assertNotNull(crr.trustRoot());

        // TODO check additional reconciles are a noop at this point

        // TODO The CO will then see the state in the trust secret and reconcile


        // TODO Which (if there are STS already) will cause a round of rolling restarts until we know that those root certs are trusted

        // We can reconcile the trust relationship
        boolean rollBroker = tc.reconcile(List.of(crr.trustRoot()));
        assertTrue(rollBroker);
        Secret secret = trustPrivateStateResource.get();
        assertNotNull(secret);
        Map<String, String> data = secret.getData();
        assertNotNull(data);
        assertEquals(
                Set.of(crr.trustRoot().fingerprint() + ".crt"),
                data.keySet());
        String c = data.get(crr.trustRoot().fingerprint() + ".crt");
        assertTrue(c != null && c.length() > 100);
        Map<String, String> stringData = secret.getStringData();
        assertNotNull(stringData);
        assertEquals(
                Set.of(crr.trustRoot().fingerprint() + ".state"),
                stringData.keySet());
        String s = stringData.get(crr.trustRoot().fingerprint() + ".state");
        assertEquals("trusted", s);


        // TODO Now the UO can reconcile again
        crr = cc.reconcile(brokerReqCert);
        assertEquals(CertificateReconcileResult.ok(), crr);
        // TODO assertions: at this point there should be ...

    }


}