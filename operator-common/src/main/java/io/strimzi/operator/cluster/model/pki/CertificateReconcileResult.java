/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

/**
 * The result of a certificate reconciliation attempt, i.e. the result type of
 * {@link CertificateController#reconcile(RequiredCertificate)}.
 * This consists of a {@link State} and optional {@link TrustRoot} representing the CA certificate that must be trusted.
 */
public class CertificateReconcileResult {

    public enum State {
        /** The requested certificate has been requested, but not yet issued. */
        WAITING_FOR_ISSUANCE,
        /** The issued certificate was bad, we can request another one, but it's likely that doing so won't help */
        ISSUED_BAD,
        /** The issued certificate is not yet trusted */
        WAITING_FOR_TRUST,
        /** An issued certificate has gone bad before it could be trusted */
        PENDING_BAD,
        /** An issued certificate is still trusted */
        OK_NOOP,
        /** A certificate was (re-)issued and is already trusted */
        OK_ISSUED
    }

    // true if there's nothing to do.
    // false if there's something to do.
    private final State state;
    // A list of trust anchors which need to be reconciled
    private final TrustRoot trustRoot;

    private static final CertificateReconcileResult OK_NOOP = new CertificateReconcileResult(State.OK_NOOP, null);
    private static final CertificateReconcileResult OK_ISSUED = new CertificateReconcileResult(State.OK_ISSUED, null);

    private CertificateReconcileResult(State state, TrustRoot trustRoot) {
        this.state = Objects.requireNonNull(state);
        this.trustRoot = trustRoot;
    }

    public static CertificateReconcileResult waitingForIssuance() {
        return new CertificateReconcileResult(State.WAITING_FOR_ISSUANCE, null);
    }

    public static CertificateReconcileResult okNoop() {
        return CertificateReconcileResult.OK_NOOP;
    }

    public static CertificateReconcileResult okIssued() {
        return CertificateReconcileResult.OK_ISSUED;
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
        return state;
    }

    public TrustRoot trustRoot() {
        return trustRoot;
    }

    @Override
    public String toString() {
        return "CertificateReconcileResult(" +
                "ok=" + state +
                ", trustRoot=" + trustRoot +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateReconcileResult that = (CertificateReconcileResult) o;
        return state == that.state &&
                Objects.equals(trustRoot, that.trustRoot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, trustRoot);
    }
}
