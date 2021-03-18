/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;

import static java.util.Arrays.asList;

/**
 * Manages the pending certificates, arranging for them to be stored somewhere during
 * {@link Pki#reconcileTrust(CustomResource, TrustManager)} and restoring them
 * during {@link Pki#reconcilePending(CustomResource, TrustManager)}.
 */
public interface PendingCertificateManager {
    class StoreException extends RuntimeException {
        public StoreException() {
        }

        public StoreException(String message) {
            super(message);
        }

        public StoreException(String message, Throwable cause) {
            super(message, cause);
        }

        public StoreException(Throwable cause) {
            super(cause);
        }
    }
    class RestoreException extends RuntimeException {
        public RestoreException() {
        }

        public RestoreException(String message) {
            super(message);
        }

        public RestoreException(String message, Throwable cause) {
            super(message, cause);
        }

        public RestoreException(Throwable cause) {
            super(cause);
        }
    }
    void storePending(List<PendingCertificate> pendingCertificates) throws StoreException;
    void restore() throws RestoreException;
}

class DefaultPendingCertificateManager implements PendingCertificateManager {
    public static final String PENDING = "pending";
    private final KubernetesClient client;
    private final String namespace;
    private final String cluster;
    private final String caSecretName;

    public DefaultPendingCertificateManager(KubernetesClient client, String namespace, String cluster, String caSecretName) {
        this.client = client;
        this.namespace = namespace;
        this.cluster = cluster;
        this.caSecretName = caSecretName;
    }

    private Resource<Secret> secretRef(String secretName) {
        return client.secrets().inNamespace(namespace).withName(secretName);
    }

    Resource<Secret> caSecretRef() {
        return secretRef(caSecretName);
    }

    @Override
    public void storePending(List<PendingCertificate> pendingCertificates) throws StoreException {
        var groupedBySecret = pendingCertificates.stream().collect(
                Collectors.groupingBy(pendingCertificate -> pendingCertificate.requiredCertificate().secretName()));
        for (var entry : groupedBySecret.entrySet()) {
            storePending(entry.getKey(), entry.getValue());
        }

        // Store a list of secret names with .pending entries in the ca secret
        String names = pendingCertificates.stream()
                .map(PendingCertificate::requiredCertificate)
                .map(RequiredCertificate::secretName)
                .distinct()
                .collect(Collectors.joining(","));
        Secret instance = caSecretRef().get();
        caSecretRef().createOrReplace((instance == null ? new SecretBuilder().withNewMetadata().withName(caSecretName).withName(namespace).endMetadata() : new SecretBuilder(instance)).addToData(PENDING,
                names).build());
    }

    /**
     * Stores an {@link IssuedCertificate} in a ".pending" secret entry encoded as pkcs#12 until the trust
     * relationship with the root CA certificate has been established.
     * The data stored here is picked up during {@link Pki#reconcilePending(CustomResource, TrustManager)}.
     * @param secretName The name of the secret to update
     * @param pendingCertificates The issued certificates to update the secret with.
     */
    private void storePending(String secretName, List<PendingCertificate> pendingCertificates) throws StoreException {
        Resource<Secret> secretResource = secretRef(secretName);
        Secret instance = secretResource.get();
        SecretBuilder secretBuilder = instance == null ? new SecretBuilder().withNewMetadata().withNamespace(namespace).withName(secretName).endMetadata() : new SecretBuilder(instance);
        for (PendingCertificate pendingCertificate : pendingCertificates) {
            IssuedCertificate issuedCertificate = pendingCertificate.issuedCertificate();
            byte[] p12bytes;
            try {
                p12bytes = issuedCertificate.toPkcs12(password());
            } catch (Exception e) {
                throw new StoreException(e);
            }
            secretBuilder.addToData(pendingCertificate.requiredCertificate().pendingCertChainEntryName(),
                    Base64.getEncoder().encodeToString(p12bytes));
        }
        secretResource.createOrReplace(secretBuilder.build());
    }

    private char[] password() {
        return ("hello " + cluster).toCharArray();
    }

    @Override
    public void restore() throws RestoreException {
        String pendingList = caSecretRef().get().getData().get(PENDING);
        if (!pendingList.isEmpty()) {
            List<String> secrets = asList(pendingList.split(","));
            // Add the issued certs to the secrets by copying the .pending to .crt, .key etc
            for (String secretName : secrets) {
                restoreFromSecret(secretName);
            }
        }
    }

    void restoreFromSecret(String secretName) throws RestoreException {
        Resource<Secret> secretResource = secretRef(secretName);
        // TODO here we just blindly copy. Perhaps the pending file should explictly the entries?
        Secret secret = secretResource.get();
        Map<String, String> data = secret.getData();
        Map<String, String> data2 = new HashMap<>(data.size());
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            if (key.endsWith(RequiredCertificate.PENDING_SUFFIX)) {
                try {
                    byte[] p12bytes = Base64.getDecoder().decode(entry.getValue());
                    IssuedCertificate issuedCertificate = IssuedCertificate.fromPkcs12(p12bytes, password());
                    String crtEntryName = key.substring(0, key.length() - RequiredCertificate.PENDING_SUFFIX.length());
                    data2.put(crtEntryName, certChainEntry(issuedCertificate));
                    data2.put(crtEntryName.replaceAll("\\.crt$", ".key"), keyEntry(issuedCertificate));
                } catch (Exception e) {
                    throw new RestoreException(e);
                }
            } else if (!data.containsKey(key + RequiredCertificate.PENDING_SUFFIX)) {
                data2.put(key, entry.getValue());
            }
        }
        secretResource.createOrReplace(new SecretBuilder(secret).withData(data2).build());
    }

    static String keyEntry(IssuedCertificate issuedCertificate) {
        return Base64.getEncoder().encodeToString(issuedCertificate.privateKey());
    }

    static String certChainEntry(IssuedCertificate issuedCertificate) throws CertificateEncodingException {
        return Base64.getEncoder().encodeToString(PkiUtils.asPemCertChain(issuedCertificate.completeChain()).getBytes(StandardCharsets.ISO_8859_1));
    }
}