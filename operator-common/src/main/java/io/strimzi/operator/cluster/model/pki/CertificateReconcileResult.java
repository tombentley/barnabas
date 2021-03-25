/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

public class CertificateReconcileResult {

    enum State {
        // A cert has been requested, but not yet issued
        WAITING_FOR_ISSUANCE,
        // The issued certificate was bad, we can request another one, but it's likely that doing so won't help
        ISSUED_BAD,
        // The issued certificate is not yet trusted
        WAITING_FOR_TRUST,
        // An issued certificate has gone bad before it could be trusted
        PENDING_BAD,
        // The issued certificate is trusted
        OK
    }

    // true if there's nothing to do.
    // false if there's something to do.
    private final State ok;
    // A list of trust anchors which need to be reconciled
    private final TrustRoot trustRoot;

    static CertificateReconcileResult OK = new CertificateReconcileResult(State.OK, null);

    private CertificateReconcileResult(State ok, TrustRoot trustRoot) {
        this.ok = Objects.requireNonNull(ok);
        this.trustRoot = trustRoot;
    }

    public static CertificateReconcileResult waitingForIssuance() {
        return new CertificateReconcileResult(State.WAITING_FOR_ISSUANCE, null);
    }

    public static CertificateReconcileResult ok() {
        return CertificateReconcileResult.OK;
    }

    public static CertificateReconcileResult waitingForTrust(TrustRoot trustRoot) {
        if (trustRoot == null) {
            throw new IllegalArgumentException("Can't have empty trustAnchors when WAITING_FOR_TRUST");
        }
        return new CertificateReconcileResult(State.WAITING_FOR_TRUST, trustRoot);
    }

    public static CertificateReconcileResult issuedBad() {
        return new CertificateReconcileResult(State.ISSUED_BAD, null);
    }

    public static CertificateReconcileResult pendingBad() {
        return new CertificateReconcileResult(State.PENDING_BAD, null);
    }

    public State state() {
        return ok;
    }

    public TrustRoot trustRoot() {
        return trustRoot;
    }

    @Override
    public String toString() {
        return "CertificateReconcileResult(" +
                "ok=" + ok +
                ", trustRoot=" + trustRoot +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateReconcileResult that = (CertificateReconcileResult) o;
        return ok == that.ok &&
                Objects.equals(trustRoot, that.trustRoot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ok, trustRoot);
    }
}
