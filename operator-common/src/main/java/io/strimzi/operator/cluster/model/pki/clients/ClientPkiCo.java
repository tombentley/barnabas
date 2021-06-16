/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustState;

/**
 * Unlike the ClusterPki, the CO part of the ClientsPki is just the trust controller part.
 * The certificates are issued by the UO.
 * Therefore this just observes certain state changes in the trust private state Secret.
 *
 * 1. When the UO issues a certificate it may create a new root cert in the trust secret
 *    in the {@link TrustState#UNTRUSTED} state.
 *    The CO rolls the brokers and transitions it to the {@link TrustState#TRUSTED} state
 *
 * 2. When a pending end entity certificate has a trusted root cert
 *    the UO can then add the issued certificate to the user secret.
 *
 * 3. When the root cert enters the renewal period the CO transitions it to PHASE_OUT.
 *
 * 4. That should cause the UO to issue new end-entity certificates if it hasn't already.
 *
 * 5. When the root certificate has expired the CO removes it from the trust state.
 *
 */
public class ClientPkiCo {

    private TrustController tc;

    public void reconcile() {
//        if (tc.reconcile()) {
//
//        }
    }
}


