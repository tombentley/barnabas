package io.strimzi.operator.cluster.model.pki;/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StrimziCertificateIssuerStoreTest {

    private KubernetesClient client;
    private Resource<Secret> secretResource;
    private StrimziCertificateIssuerStore issuer;

    @BeforeEach
    public void setup() {
        client = new MockKube().build();
        secretResource = client.secrets().inNamespace("namespace").withName(KafkaResources.clusterCaKeySecretName("cluster"));
        issuer = new StrimziCertificateIssuerStore(Clock.systemUTC(), secretResource);
    }

    @Test
    public void testBootstrapCaWithCaDefaults() throws CertificateException {
        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName("cluster")
                    .withUid("123")
                .endMetadata()
                .build();

        issuer.bootstrapCa(cluster);

        Secret secret = secretResource.get();
        assertEquals(1, secret.getMetadata().getOwnerReferences().size());
        assertEquals("Kafka", secret.getMetadata().getOwnerReferences().get(0).getKind());
        Map<String, String> data = secret.getData();
        assertEquals(Set.of("ca.crt", "ca.key"), data.keySet());

        issuer.bootstrapCa(cluster);

        secret = secretResource.get();
        assertEquals(1, secret.getMetadata().getOwnerReferences().size());
        assertEquals("Kafka", secret.getMetadata().getOwnerReferences().get(0).getKind());
        assertEquals(data, secret.getData());
    }

    @Test
    public void testBootstrapCa() throws CertificateException {
        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                .withName("cluster")
                .withUid("123")
                .endMetadata()
                .withNewSpec()
                    .withNewClusterCa()
                        .withValidityDays(10)
                        .withRenewalDays(5)
                        .withGenerateSecretOwnerReference(false)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                    .endClusterCa()
                .endSpec()
                .build();

        issuer.bootstrapCa(cluster);

        Secret secret = secretResource.get();
        assertEquals(0, secret.getMetadata().getOwnerReferences().size());
        Map<String, String> data = secret.getData();
        assertEquals(Set.of("ca.crt", "ca.key"), data.keySet());

        issuer.bootstrapCa(cluster);

        secret = secretResource.get();
        assertEquals(0, secret.getMetadata().getOwnerReferences().size());
        assertEquals(data, secret.getData());
    }

    // TODO test what happens when the root CA expires (by adjusting the clock)

    // TODO test with intermediate CA (and when that expires)

}