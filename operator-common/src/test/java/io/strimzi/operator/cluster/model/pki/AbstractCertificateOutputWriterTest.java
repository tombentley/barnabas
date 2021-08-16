/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AbstractCertificateOutputWriterTest {

    private Resource<Secret> secretResource;
    private CertificateOutputWriter<TestId> writer;

    @BeforeEach
    public void setup() {
        var op = new MockKube().build().secrets().inNamespace("ns");
        secretResource = op.withName("n");
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(TestId.class, "broker"));
        writer = new AbstractCertificateOutputWriter<TestId>(
                new Reconciliation("test", "Kafka", "ns", "n"),

                null, mapper) {
            @Override
            protected String keyPrefix(TestId id) {
                return id.name();
            }

            protected String p12Key(TestId id) {
                return null;
            }

            protected String passwordKey(TestId id) {
                return null;
            }

            @Override
            protected NamedResource<Secret> statusResource(TestId subjectIdentity) {
                return new NamedResource<>(op, "n");
            }

            @Override
            protected NamedResource<Secret> issuedResource(TestId subjectIdentity) {
                return new NamedResource<>(op, "n");
            }
        };
    }

    @Test
    public void testRoundtripStatus() throws IOException, CertificateException, InvalidKeySpecException, KeyStoreException {
        TestId subjectIdentity = new TestId("0");

        var loaded = writer.loadStatus(subjectIdentity);
        Assertions.assertEquals(CertificateState.NEW, loaded.state());

        byte[] token = {1, 2, 3, 4};
        writer.storeRequested(subjectIdentity, token);
        assertEquals(Set.of("0.requested"), secretResource.get().getData().keySet());
        loaded = writer.loadStatus(subjectIdentity);
        assertEquals(CertificateState.REQUESTED, loaded.state());
        assertArrayEquals(token, loaded.tokenBytes());
        assertNull(loaded.issuedCertificate());

        IssuedCertificate issuedCertificate = CertHelper.issuedCertificate();
        writer.storePending(subjectIdentity, issuedCertificate);
        assertEquals(Set.of("0.pending"), secretResource.get().getData().keySet());
        loaded = writer.loadStatus(subjectIdentity);
        assertEquals(CertificateState.PENDING, loaded.state());
        assertNull(loaded.tokenBytes());
        assertEquals(issuedCertificate, loaded.issuedCertificate());

        writer.writeIssued(subjectIdentity, issuedCertificate);
        assertEquals(Set.of("0.crt", "0.key"), secretResource.get().getData().keySet());
        loaded = writer.loadStatus(subjectIdentity);
        assertEquals(CertificateState.IN_USE, loaded.state());

        writer.storeRequested(subjectIdentity, token);
        assertEquals(Set.of("0.crt", "0.key", "0.requested"), secretResource.get().getData().keySet());
        loaded = writer.loadStatus(subjectIdentity);
        assertEquals(CertificateState.REQUESTED, loaded.state());
        assertArrayEquals(token, loaded.tokenBytes());
        assertNull(loaded.issuedCertificate());

        writer.storePending(subjectIdentity, issuedCertificate);
        assertEquals(Set.of("0.crt", "0.key", "0.pending"), secretResource.get().getData().keySet());
        loaded = writer.loadStatus(subjectIdentity);
        assertEquals(CertificateState.PENDING, loaded.state());
        assertNull(loaded.tokenBytes());
        assertEquals(issuedCertificate, loaded.issuedCertificate());

    }
}