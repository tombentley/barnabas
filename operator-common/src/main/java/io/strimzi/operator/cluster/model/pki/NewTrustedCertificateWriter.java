/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * A TrustOutputWriter impl supporting the new layout, suitable for
 * {@code $CLUSTER-$CA-trust}.
 */
public class NewTrustedCertificateWriter implements TrustedCertificateWriter {

    private final Resource<Secret> secretResource;

    public NewTrustedCertificateWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Map<Fingerprint, X509Certificate> trustRoots) throws CertificateEncodingException {
        Map<String, String> data = new HashMap<>();
        for (Map.Entry<Fingerprint, X509Certificate> root : trustRoots.entrySet()) {
            data.put(root.getKey() + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(root.getValue())));
            // TODO data.put(root.fingerprint() + ".p12", Base64.getEncoder().encodeToString(PkiUtils.asP12(root.x509())));
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret)).withData(data).build());
    }
}
