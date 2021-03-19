/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Abstraction for things which know how to take a bunch of {@code TrustRoot TrustRoots}
 * and put them somewhere. This includes, but is not limited to, putting them in a Secret.
 */
public interface TrustOutputWriter {
    void write(Collection<TrustRoot> trustRoot) throws CertificateEncodingException;
}
/**
 * A TrustOutputWriter impl supporting the legacy layout, suitable for
 * {@code $CLUSTER-cluster-ca-certs} and {@code $CLUSTER-client-ca-certs}.
 */
class LegacyTrustOutputWriter implements TrustOutputWriter {

    private final Resource<Secret> secretResource;

    public LegacyTrustOutputWriter(String name, Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Collection<TrustRoot> trustRoot) throws CertificateEncodingException {
        List<TrustRoot> collect = trustRoot.stream().sorted(Comparator.comparing(TrustRoot::notAfter)).collect(Collectors.toList());
        Map<String, String> data = new HashMap<>();
        if (!collect.isEmpty()) {
            TrustRoot last = collect.remove(collect.size() - 1);
            data.put("ca.crt", Base64.getEncoder().encodeToString(PkiUtils.asPemString(last.x509())));
            for (TrustRoot root : collect) {
                data.put("ca-data.crt", Base64.getEncoder().encodeToString(PkiUtils.asPemString(last.x509())));
            }
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret).withData(data)).build());
    }
}
/**
 * A TrustOutputWriter impl supporting the new layout, suitable for
 * {@code $CLUSTER-$CA-trust}.
 */
class NewTrustOutputWriter implements TrustOutputWriter {

    private final Resource<Secret> secretResource;

    public NewTrustOutputWriter(String name, Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Collection<TrustRoot> trustRoots) throws CertificateEncodingException {
        Map<String, String> data = new HashMap<>();
        for (TrustRoot root : trustRoots) {
            data.put(root.fingerprint() + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemString(root.x509())));
            data.put(root.fingerprint() + ".p12", Base64.getEncoder().encodeToString(PkiUtils.asP12(root.x509())));
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret).withData(data)).build());
    }
}
