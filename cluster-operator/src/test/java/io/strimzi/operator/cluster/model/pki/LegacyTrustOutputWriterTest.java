/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LegacyTrustOutputWriterTest {

    @Test
    public void testWrite() throws IOException, CertificateException {
        Resource<Secret> secretResource = new MockKube().build().secrets().inNamespace("ns").withName("name");
        LegacyTrustOutputWriter writer = new LegacyTrustOutputWriter(secretResource);

        TrustRoot root1 = new TrustRoot(TrustPrivateStateTest.getX509Certificate("/CLUSTER_CERT.pem"), TrustState.UNTRUSTED);
        TrustRoot root2 = new TrustRoot(TrustPrivateStateTest.getX509Certificate("/CLIENTS_CERT.pem"), TrustState.TRUSTED);
        writer.write(List.of(root1, root2));

        assertEquals(
                Set.of(
                        "ca.crt",
                        "ca-2118-07-30T16-19-54Z.crt"),
                secretResource.get().getData().keySet());
    }

}