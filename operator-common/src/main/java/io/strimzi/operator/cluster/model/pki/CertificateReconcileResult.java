/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

/**
 * The result of a certificate reconciliation attempt, i.e. the result type of
 * {@link CertificateController#reconcile(RequiredCertificate)}.
 */
public abstract class CertificateReconcileResult {

    private CertificateReconcileResult() {

    }

    /**
     * The requested certificate has been requested, but not yet issued.
     */
    public static final class WaitingForIssuance extends CertificateReconcileResult {
        public WaitingForIssuance() {

        }

        @Override
        public String toString() {
            return "WaitingForIssuance{}";
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof WaitingForIssuance;
        }
    }

    /**
     * The issued certificate was bad, we can request another one,
     * but it's likely that doing so won't help.
     */
    public static final class IssuedBad extends CertificateReconcileResult {
        public IssuedBad() {

        }

        @Override
        public String toString() {
            return "IssuedBad{}";
        }

        @Override
        public int hashCode() {
            return 2;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof IssuedBad;
        }
    }

    /**
     * An issued certificate has gone bad before it could be trusted.
     */
    public static final class PendingBad extends CertificateReconcileResult {
        public PendingBad() {

        }

        @Override
        public String toString() {
            return "PendingBad{}";
        }

        @Override
        public int hashCode() {
            return 3;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof PendingBad;
        }
    }

    /**
     * The issued certificate is not yet trusted.
     * There <strong>is</strong> a {@link #trustRoot()} associated with results in this state.
     */
    public static final class WaitingForTrust extends CertificateReconcileResult {
        private final TrustRoot trustRoot;

        public WaitingForTrust(TrustRoot trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public TrustRoot trustRoot() {
            return trustRoot;
        }

        @Override
        public String toString() {
            return "WaitingForTrust(" +
                    "trustRoot.fingerprint=" + trustRoot.fingerprint() +
                    ", trustRoot.state=" + trustRoot.state() +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WaitingForTrust that = (WaitingForTrust) o;
            return Objects.equals(trustRoot, that.trustRoot);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trustRoot);
        }
    }

    /**
     * A previously-issued certificate is still trusted.
     * There is no {@link #trustRoot()} associated with results in this state.
     */
    public static final class OkNoop extends CertificateReconcileResult {
        private final TrustRoot trustRoot;

        public OkNoop(TrustRoot trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public TrustRoot trustRoot() {
            return trustRoot;
        }

        @Override
        public String toString() {
            return "OkNoop(" +
                    "trustRoot.fingerprint=" + trustRoot.fingerprint() +
                    ", trustRoot.state=" + trustRoot.state() +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OkNoop okNoop = (OkNoop) o;
            return Objects.equals(trustRoot, okNoop.trustRoot);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trustRoot);
        }
    }

    /**
     * A certificate was (re-)issued and is already trusted.
     * There is no {@link #trustRoot()} associated with results in this state.
     */
    public static final class OkIssued extends CertificateReconcileResult {
        private final TrustRoot trustRoot;

        public OkIssued(TrustRoot trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public TrustRoot trustRoot() {
            return trustRoot;
        }

        @Override
        public String toString() {
            return "OkIssued(" +
                    "trustRoot.fingerprint=" + trustRoot.fingerprint() +
                    ", trustRoot.state=" + trustRoot.state() +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OkIssued okIssued = (OkIssued) o;
            return Objects.equals(trustRoot, okIssued.trustRoot);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trustRoot);
        }
    }
}
