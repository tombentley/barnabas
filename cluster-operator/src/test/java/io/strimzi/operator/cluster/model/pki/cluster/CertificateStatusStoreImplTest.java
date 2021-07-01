/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.CertificateState;
import io.strimzi.operator.cluster.model.pki.CertificateStatus;
import io.strimzi.operator.cluster.model.pki.IssuedCertificate;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.TrustPrivateStateTest;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CertificateStatusStoreImplTest {

    private Resource<Secret> secretResource;
    private CertificateStatusStoreImpl writer;

    @BeforeEach
    public void setup() {
        var op = new MockKube().build().secrets().inNamespace("ns");
        secretResource = op.withName("n");
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(BrokerIdentity.class, "broker"));
        writer = new CertificateStatusStoreImpl(new Reconciliation("test", "Kafka", "ns", "n"), mapper, null,
                 "n",
                 null,
                 new NamedResource<>(op, "n"),
                 null,
                 null,
                 null,
                 null);
    }

    @Test
    public void testRoundtripStatus() throws IOException, CertificateException {
        BrokerIdentity subjectIdentity = new BrokerIdentity(0);
        var cs = CertificateStatus.createNew(subjectIdentity);
        Assertions.assertEquals(cs.state(), CertificateState.NEW);
        writer.storeStatus(cs);
        System.err.println(secretResource.get().getData().get("broker-0.state"));
        CertificateStatus loaded = writer.loadStatus(cs.subjectIdentity());
        assertEquals(cs, loaded);
        assertEquals(subjectIdentity, loaded.subjectIdentity());

        byte[] token = {1, 2, 3};
        cs = cs.toRequested(token);
        assertEquals(cs.state(), CertificateState.REQUESTED);
        assertEquals(token, cs.tokenBytes());
        writer.storeStatus(cs);
        System.err.println(secretResource.get().getData().get("broker-0.state"));
        loaded = writer.loadStatus(cs.subjectIdentity());
        assertEquals(cs, loaded);
        assertEquals(subjectIdentity, loaded.subjectIdentity());

        cs = cs.toPending(issuedCertificate());
        assertEquals(cs.state(), CertificateState.PENDING);
        writer.storeStatus(cs);
        System.err.println(secretResource.get().getData().get("broker-0.state"));
        loaded = writer.loadStatus(cs.subjectIdentity());
        assertEquals(cs, loaded);
        assertEquals(subjectIdentity, loaded.subjectIdentity());

        cs = cs.toInUse();
        assertEquals(cs.state(), CertificateState.IN_USE);
        writer.storeStatus(cs);
        System.err.println(secretResource.get().getData().get("broker-0.state"));
        loaded = writer.loadStatus(cs.subjectIdentity());
        assertEquals(cs, loaded);
        assertEquals(subjectIdentity, loaded.subjectIdentity());

        cs = cs.toNotNeeded();
        assertEquals(cs.state(), CertificateState.NOT_NEEDED);
        writer.storeStatus(cs);
        System.err.println(secretResource.get().getData().get("broker-0.state"));
        loaded = writer.loadStatus(cs.subjectIdentity());
        assertEquals(cs, loaded);
        assertEquals(subjectIdentity, loaded.subjectIdentity());

    }

    IssuedCertificate issuedCertificate() throws IOException, CertificateException {
        X509Certificate x509Certificate = TrustPrivateStateTest.getX509Certificate("/CLUSTER_CERT.pem");
        return new IssuedCertificate(new byte[]{4, 5, 6}, x509Certificate, List.of(x509Certificate), x509Certificate);
    }
}