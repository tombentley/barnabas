/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.OpensslCaTestHarness;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokersCertificateOutputWriterTest extends OpensslCaTestHarness {

    private Resource<Secret> secretResource;
    private BrokersCertificateOutputWriter writer;

    @BeforeEach
    public void setup() {
        String ns = "ns";
        String cluster = "cluster";
        String secretName = "n";
        var op = new MockKube().build().secrets().inNamespace(ns);
        secretResource = op
                .withName(secretName);
        writer = new BrokersCertificateOutputWriter(new Reconciliation("test", "Kafka", ns, cluster),
                cluster,
                new NamedResource<>(op, secretName), true, true);
    }



    @Test
    public void testWriteIssued() throws IOException, CertificateException, InvalidKeySpecException, KeyStoreException {
        writer.writeIssued(new BrokerIdentity(0), issuedCertificate(), null);
        assertEquals(
                Set.of(
                        "cluster-kafka-0.p12", "cluster-kafka-0.password", "cluster-kafka-0.crt", "cluster-kafka-0.key"),
                secretResource.get().getData().keySet());

        writer.writeIssued(new BrokerIdentity(1), issuedCertificate(), null);
        assertEquals(
                Set.of(
                    "cluster-kafka-0.p12", "cluster-kafka-0.password", "cluster-kafka-0.crt", "cluster-kafka-0.key",
                    "cluster-kafka-1.p12", "cluster-kafka-1.password", "cluster-kafka-1.crt", "cluster-kafka-1.key"),
                secretResource.get().getData().keySet());
    }
}