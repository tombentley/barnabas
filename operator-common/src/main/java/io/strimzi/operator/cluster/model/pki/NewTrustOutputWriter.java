/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * A TrustOutputWriter impl supporting the new layout, suitable for
 * {@code $CLUSTER-$CA-trust}.
 */
public class NewTrustOutputWriter implements TrustOutputWriter {

    private final Resource<Secret> secretResource;

    public NewTrustOutputWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Collection<TrustRoot> trustRoots) throws CertificateEncodingException {
        Map<String, String> data = new HashMap<>();
        for (TrustRoot root : trustRoots) {
            data.put(root.fingerprint() + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(root.x509())));
            // TODO data.put(root.fingerprint() + ".p12", Base64.getEncoder().encodeToString(PkiUtils.asP12(root.x509())));
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret)).withData(data).build());
    }
}
