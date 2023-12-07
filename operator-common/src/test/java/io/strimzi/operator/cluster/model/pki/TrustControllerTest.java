/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrustControllerTest {

    private static X509Certificate cert;
    private static Fingerprint fingerprint;
    private KubernetesClient client;
    private Reconciliation reconciliation = new Reconciliation("test", "Kafka", "ns", "a");

    private static byte[] certBytes() throws IOException {
        InputStream resourceAsStream = PkiUtilsTest.class.getResourceAsStream("/CLUSTER_CERT.pem");
        return resourceAsStream.readAllBytes();
    }

    @BeforeAll
    public static void x509Certificate() throws CertificateException, IOException {
        byte[] buffer = certBytes();
        cert = PkiUtils.x509Certificate(buffer);
        fingerprint = PkiUtils.fingerprint(cert);
    }

    @BeforeEach
    public void before() {
        client = new MockKube().build();
    }

    @Test
    public void testExpectedLifecycle() {
        var tc = new TrustController(reconciliation, new NamedResource<>(client.secrets().inNamespace("foo"), "bar"), null);
        assertEquals(Set.of(), tc.requiringTrust());
        assertEquals(Set.of(), tc.zombies());
        assertNull(client.secrets().inNamespace("foo").withName("bar").get());
        assertNull(tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(), tc.rootCertsInStates(Set.of(TrustState.UNTRUSTED)));

        tc.requireTrust(cert);
        assertEquals(Set.of(cert), tc.requiringTrust());
        assertEquals(Set.of(), tc.zombies());
        assertEquals(TrustState.UNTRUSTED, tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(cert), tc.rootCertsInStates(Set.of(TrustState.UNTRUSTED)));
        Map<String, String> data = client.secrets().inNamespace("foo").withName("bar").get().getData();
        assertEquals(Set.of("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.crt", "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state"),
                data.keySet());
        assertEquals(TrustState.UNTRUSTED,
                TrustController.decodeState(data.get("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state")));

        tc.trustRootCerts(Set.of(cert));
        assertEquals(Set.of(), tc.requiringTrust());
        assertEquals(Set.of(), tc.zombies());
        assertEquals(TrustState.TRUSTED_UNUSED, tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(cert), tc.rootCertsInStates(Set.of(TrustState.TRUSTED_UNUSED)));
        data = client.secrets().inNamespace("foo").withName("bar").get().getData();
        assertEquals(Set.of("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.crt", "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state"),
                data.keySet());
        assertEquals(TrustState.TRUSTED_UNUSED,
                TrustController.decodeState(data.get("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state")));

        tc.depend(cert);
        assertEquals(Set.of(), tc.requiringTrust());
        assertEquals(Set.of(), tc.zombies());
        assertEquals(TrustState.TRUSTED_USED, tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(cert), tc.rootCertsInStates(Set.of(TrustState.TRUSTED_USED)));
        data = client.secrets().inNamespace("foo").withName("bar").get().getData();
        assertEquals(Set.of("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.crt", "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state"),
                data.keySet());
        assertEquals(TrustState.TRUSTED_USED,
                TrustController.decodeState(data.get("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state")));

        tc.undepend(cert);
        assertEquals(Set.of(), tc.requiringTrust());
        assertEquals(Set.of(cert), tc.zombies());
        assertEquals(TrustState.ZOMBIE, tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(cert), tc.rootCertsInStates(Set.of(TrustState.ZOMBIE)));
        data = client.secrets().inNamespace("foo").withName("bar").get().getData();
        assertEquals(Set.of("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.crt", "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state"),
                data.keySet());
        assertEquals(TrustState.ZOMBIE,
                TrustController.decodeState(data.get("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state")));

        tc.delete(Set.of(cert));
        assertEquals(Set.of(), tc.requiringTrust());
        assertEquals(Set.of(), tc.zombies());
        assertNull(tc.stateOfRootCert(fingerprint));
        assertEquals(Set.of(), tc.rootCertsInStates(EnumSet.allOf(TrustState.class)));
        data = client.secrets().inNamespace("foo").withName("bar").get().getData();
        assertEquals(Set.of(), data.keySet());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConflictHandling() {
        NamedResource<Secret> resource = mock(NamedResource.class);
        when(resource.get()).thenReturn(new SecretBuilder().withNewMetadata().withResourceVersion("123").endMetadata().build());
        NamedResource<Secret> lockedResource = mock(NamedResource.class);
        when(resource.lockResourceVersion(any())).thenReturn(lockedResource);
        AtomicInteger count = new AtomicInteger(3);
        when(lockedResource.replace(any())).thenAnswer(i -> {
            if (count.decrementAndGet() == 0) {
                return i.getArgument(0);
            } else {
                throw new KubernetesClientException("Conflict", 409, null);
            }
        });
        var tc = new TrustController(reconciliation, resource, null);
        tc.requireTrust(cert);
        assertEquals(0, count.get());
    }

}