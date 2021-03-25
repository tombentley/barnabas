/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Objects;

import io.strimzi.api.kafka.model.status.CaCertState;

/**
 * A CA certificate together with it's current state.
 * {@code TrustRoot} is, perhaps, a slight misnomer, since the certificate is not necessarily
 * {@link CaCertState#TRUSTED}.
 *
 * @see TrustController which manages the state of a TrustRoot within a given "CA".
 */
class TrustRoot {

    private final TrustState state;
    private final X509Certificate x509;
    private Fingerprint fingerprint = null;

    public TrustRoot(X509Certificate x509, TrustState state) {
        this.state = Objects.requireNonNull(state);
        this.x509 = Objects.requireNonNull(x509);
    }

    public Fingerprint fingerprint() {
        return fingerprint == null ? fingerprint = PkiUtils.fingerprint(x509()) : fingerprint;
    }

    X509Certificate x509() {
        return x509;
    }

    public TrustState state() {
        return state;
    }

    public Instant notBefore() {
        return x509().getNotBefore().toInstant();
    }

    public Instant notAfter() {
        return x509().getNotAfter().toInstant();
    }

    public boolean isValidAt(Instant time) {
        return time.isAfter(notBefore()) && time.isBefore(notAfter());
    }

    @Override
    public String toString() {
        return "TrustRoot(" +
                "state=" + state +
                ", x509=" + x509 +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrustRoot trustRoot = (TrustRoot) o;
        return state == trustRoot.state &&
                Objects.equals(x509, trustRoot.x509);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, x509);
    }
}
