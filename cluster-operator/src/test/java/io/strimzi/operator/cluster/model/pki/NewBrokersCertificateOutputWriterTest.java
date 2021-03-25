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

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NewBrokersCertificateOutputWriterTest extends OpensslCaTestHarness {

    private Resource<Secret> secretResource;
    private NewBrokersCertificateOutputWriter writer;

    @BeforeEach
    public void setup() {
        secretResource = new MockKube().build().secrets().inNamespace("ns")
                .withName("n");
        writer = new NewBrokersCertificateOutputWriter(secretResource);
    }



    @Test
    public void testWriteIssued() throws IOException, CertificateException, InvalidKeySpecException, KeyStoreException {
        writer.writeIssued(new BrokerIdentity(0), issuedCertificate());
        assertEquals(
                Set.of(
                        "broker-0.p12", "broker-0.password"),
                secretResource.get().getData().keySet());

        writer.writeIssued(new BrokerIdentity(1), issuedCertificate());
        assertEquals(
                Set.of(
                    "broker-0.p12", "broker-0.password",
                    "broker-1.p12", "broker-1.password"),
                secretResource.get().getData().keySet());
    }
}