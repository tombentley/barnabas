/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.CertHelper;
import io.strimzi.operator.cluster.model.pki.LegacyTrustedCertificateWriter;
import io.strimzi.operator.cluster.model.pki.PkiUtils;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LegacyTrustedCertificateWriterTest {

    @Test
    public void testWrite() throws IOException, CertificateException {
        Resource<Secret> secretResource = new MockKube().build().secrets().inNamespace("ns").withName("name");
        LegacyTrustedCertificateWriter writer = new LegacyTrustedCertificateWriter(secretResource);

        var root1 = CertHelper.getX509Certificate("/CLUSTER_CERT.pem");
        var root2 = CertHelper.getX509Certificate("/CLIENTS_CERT.pem");
        writer.write(Map.of(PkiUtils.fingerprint(root1), root1,
                PkiUtils.fingerprint(root2), root2));

        assertEquals(
                Set.of(
                        "ca.crt",
                        "ca-2118-07-30T16-19-54Z.crt"),
                secretResource.get().getData().keySet());
    }

}