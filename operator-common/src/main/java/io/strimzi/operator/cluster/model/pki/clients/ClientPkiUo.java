/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import java.security.cert.CertificateException;

import io.strimzi.operator.cluster.model.pki.CertificateController;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TrustState;

/**
 * Unlike the ClusterPki, the UO part of the ClientsPki is just the certificate controller part.
 * The trust in root CA certificates is maintained by the CO.
 * Therefore this just observes issued certificates and awaits certain state changes in the trust private state Secret
 * which are performed to the CO.
 * <p>
 * 1. When the UO issues a certificate it may create a new root cert in the trust secret
 * in the {@link TrustState#UNTRUSTED} state.
 * The CO rolls the brokers and transitions it to the {@link TrustState#TRUSTED} state
 * <p>
 * 2. When a pending end entity certificate has a trusted root cert
 * the UO can then add the issued certificate to the user secret.
 * <p>
 * 3. When the root cert enters the renewal period the CO transitions it to PHASE_OUT.
 * <p>
 * 4. That should cause the UO to issue new end-entity certificates if it hasn't already.
 * <p>
 * 5. When the root certificate has expired the CO removes it from the trust state.
 */
public class ClientPkiUo {

    private CertificateController cc;

    public CertificateReconcileResult reconcile(RequiredCertificate requiredCertificate) throws CertificateException {
        return cc.reconcile(requiredCertificate);
    }

    public void gc() {
        //cc.
    }
}
