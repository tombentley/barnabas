/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Objects;

/**
 * A CA certificate together with it's current state.
 * {@code TrustRoot} is, perhaps, a slight misnomer, since the certificate is not necessarily
 * {@link TrustState#TRUSTED}.
 *
 * @see TrustController which manages the transitions UNTRUSTED -&gt; TRUSTED and others.
 * @see CertificateController which creates TrustRoots in the UNTRUSTED state and is responsible for the other transitions.
 * @see TrustPrivateState which manages the persistent state and is the means by which the two controllers communicate.
 */
public class TrustRoot {

    private final TrustState state;
    private final X509Certificate x509;
    private Fingerprint fingerprint = null;
    /* Invariant: The only time that resourceVersion may be null is when the CC observes a new root CA cert
     * and adds it to the Secret via the TrustPrivateState.
     * All the TrustRoots loaded from TrustPrivateState have a resourceVersion set, and it is preserved for
     * all transitions in memory so that TrustPrivateState can use that version for optimistic locking when
     * updating the state.
     */
    private final String resourceVersion;

    TrustRoot(X509Certificate x509, TrustState state, String resourceVersion) {
        this.state = Objects.requireNonNull(state);
        this.x509 = Objects.requireNonNull(x509);
        if (resourceVersion == null && state != TrustState.UNTRUSTED) {
            throw new IllegalStateException();
        }
        this.resourceVersion = resourceVersion;
    }

    /**
     * Intended only to be called by {@link TrustPrivateState}.
     * @param x509 The cert.
     * @param state Its state.
     * @param resourceVersion The metadata.resourceVersion of the Secret from which it was loaded.
     * @return
     */
    static TrustRoot load(X509Certificate x509, TrustState state, String resourceVersion) {
        Objects.requireNonNull(resourceVersion);
        return new TrustRoot(x509, state, resourceVersion);
    }

    public static TrustRoot untrusted(X509Certificate x509) {
        return new TrustRoot(x509, TrustState.UNTRUSTED, null);
    }

    public TrustRoot toTrusted(String resourceVersion) {
        return new TrustRoot(x509, TrustState.TRUSTED, resourceVersion);
    }

    public TrustRoot toPhaseOut() {
        return new TrustRoot(x509, TrustState.PHASE_OUT, this.resourceVersion);
    }

    public TrustRoot toZombie() {
        return new TrustRoot(x509, TrustState.ZOMBIE, this.resourceVersion);
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

    public String resourceVersion() {
        return resourceVersion;
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
                "resourceVersion=" + resourceVersion +
                ", state=" + state +
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
