/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.List;

import io.fabric8.kubernetes.client.CustomResource;

/**
 * Manages the pending certificates, arranging for them to be stored somewhere during
 * {@link Pki#reconcileTrust(CustomResource, TrustManager)} and restoring them
 * during {@link Pki#reconcilePending(CustomResource, TrustManager)}.
 */
public interface PendingCertificateManager {
    void storePending(List<PendingCertificate> pendingCertificates) throws StoreException;
    void restore() throws RestoreException;
}

