/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;

/**
 * The result of a certificate reconciliation attempt, i.e. the result type of
 * {@link CertificateController#reconcile(RequiredCertificate, CertificateOutputWriter)}.
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

        @Override
        public Optional<X509Certificate> trustRoot() {
            return Optional.empty();
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

        @Override
        public Optional<X509Certificate> trustRoot() {
            return Optional.empty();
        }
    }

    /**
     * An issued certificate has gone bad before it could be trusted.
     */
    public static final class PendingBad extends CertificateReconcileResult {
        public PendingBad() {

        }

        @Override
        public Optional<X509Certificate> trustRoot() {
            return Optional.empty();
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
        private final X509Certificate trustRoot;

        public WaitingForTrust(X509Certificate trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public Optional<X509Certificate> trustRoot() {
            return Optional.of(trustRoot);
        }

        @Override
        public String toString() {
            return "WaitingForTrust(" +
                    "trustRoot.fingerprint=" + PkiUtils.fingerprint(trustRoot) +
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
        private final X509Certificate trustRoot;

        public OkNoop(X509Certificate trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public Optional<X509Certificate> trustRoot() {
            return Optional.of(trustRoot);
        }

        @Override
        public String toString() {
            return "OkNoop(" +
                    "trustRoot.fingerprint=" + PkiUtils.fingerprint(trustRoot) +
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
        private final X509Certificate trustRoot;

        public OkIssued(X509Certificate trustRoot) {
            Objects.requireNonNull(trustRoot);
            this.trustRoot = trustRoot;
        }

        public Optional<X509Certificate> trustRoot() {
            return Optional.of(trustRoot);
        }

        @Override
        public String toString() {
            return "OkIssued(" +
                    "trustRoot.fingerprint=" + PkiUtils.fingerprint(trustRoot) +
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


    public abstract Optional<X509Certificate> trustRoot();
}
