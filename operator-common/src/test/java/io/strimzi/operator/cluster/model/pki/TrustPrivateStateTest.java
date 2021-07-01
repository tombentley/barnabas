/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TrustPrivateStateTest {


    public static X509Certificate getX509Certificate(String s) throws IOException, CertificateException {
        InputStream resourceAsStream = PkiUtilsTest.class.getResourceAsStream(s);
        byte[] buffer = resourceAsStream.readAllBytes();
        return PkiUtils.x509Certificate(buffer);
    }

    @Test
    public void test1() throws CertificateException, IOException {
        var op = new MockKube().build().secrets().inNamespace("ns");
        Resource<Secret> secretResource = op.withName("trust");
        TrustPrivateState tps = new TrustPrivateState(null, new NamedResource<>(op, "trust"));
        TrustRoot root1 = TrustRoot.untrusted(getX509Certificate("/CLUSTER_CERT.pem"));
        TrustRoot root2 = TrustRoot.untrusted(getX509Certificate("/CLIENTS_CERT.pem")).toTrusted("123");
        Map<Fingerprint, TrustRoot> stored = Map.of(root1.fingerprint(), root1,
                root2.fingerprint(), root2);
        tps.transition(stored);

        assertEquals(
                Set.of("7B779A0F752941046382D8135F0D4891CA593CFA.crt",
                        "7B779A0F752941046382D8135F0D4891CA593CFA.state",
                        "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.crt",
                    "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state"),
                secretResource.get().getData().keySet());

        assertEquals("trusted",
                new String(Base64.getDecoder().decode(
                        secretResource.get().getData().get("7B779A0F752941046382D8135F0D4891CA593CFA.state")),
                        StandardCharsets.US_ASCII));

        assertEquals(
                "untrusted",
                new String(Base64.getDecoder().decode(
                        secretResource.get().getData().get("9B7EBFFEAE1B1F86A9991625E561C52D3779E49C.state")),
                        StandardCharsets.US_ASCII));

        Map<Fingerprint, TrustRoot> loaded = tps.load();
        assertEquals(stored, loaded);
    }
}