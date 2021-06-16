/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.LegacyTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.TrustPrivateStateTest;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LegacyTrustOutputWriterTest {

    @Test
    public void testWrite() throws IOException, CertificateException {
        Resource<Secret> secretResource = new MockKube().build().secrets().inNamespace("ns").withName("name");
        LegacyTrustOutputWriter writer = new LegacyTrustOutputWriter(secretResource);

        TrustRoot root1 = TrustRoot.untrusted(TrustPrivateStateTest.getX509Certificate("/CLUSTER_CERT.pem"));
        TrustRoot root2 = TrustRoot.untrusted(TrustPrivateStateTest.getX509Certificate("/CLIENTS_CERT.pem")).toTrusted("123");
        writer.write(List.of(root1, root2));

        assertEquals(
                Set.of(
                        "ca.crt",
                        "ca-2118-07-30T16-19-54Z.crt"),
                secretResource.get().getData().keySet());
    }

}