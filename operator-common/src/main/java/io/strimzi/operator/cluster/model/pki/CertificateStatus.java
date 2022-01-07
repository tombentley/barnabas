/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A certificate and its current state.
 * Instances can be represented as JSON and persisted via a {@link CertificateOutputWriter}.
 * The storage of CertificateStatus instances is for internal use by the {@link CertificateController},
 * and not for use by components (e.g. brokers) needing to use a certificate, for which there is a separate
 * interface, {@link CertificateOutputWriter}.
 */
public class CertificateStatus {
    private final CertificateState state;
    private final IssuedCertificate issuedCertificate;
    private final byte[] token;

    @JsonCreator
    public CertificateStatus(
            @JsonProperty("state") CertificateState state,
            @JsonProperty("requestToken") byte[] token,
            @JsonProperty("pending") IssuedCertificate pending) {
        this.state = Objects.requireNonNull(state);
        this.issuedCertificate = pending;
        this.token = token;
    }

    public static CertificateStatus createNew() {
        return new CertificateStatus(CertificateState.REQUIRED, null, null);
    }

    public CertificateStatus toRequested(byte[] token) {
        if (this.state != CertificateState.REQUIRED && this.state != CertificateState.IN_USE)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.REQUESTED);
        return new CertificateStatus(CertificateState.REQUESTED, token, null);
    }

    public CertificateStatus toPending(IssuedCertificate issuedCertificate) {
        Objects.requireNonNull(issuedCertificate);
        if (this.state != CertificateState.REQUESTED)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.PENDING);
        return new CertificateStatus(CertificateState.PENDING, null, issuedCertificate);
    }

    public CertificateStatus toInUse() {
        if (this.state != CertificateState.PENDING)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.IN_USE);
        return new CertificateStatus(CertificateState.IN_USE, null, this.issuedCertificate);
    }

    public CertificateStatus toNotNeeded() {
        if (this.state != CertificateState.IN_USE)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.NOT_NEEDED);
        return new CertificateStatus(CertificateState.NOT_NEEDED, null, null);
    }

    @JsonProperty("requestToken")
    public byte[] tokenBytes() {
        if (state == CertificateState.REQUESTED) {
            return token;
        } else {
            return null;
        }
    }


    @JsonProperty
    public CertificateState state() {
        return state;
    }

    @JsonProperty("pending")
    public IssuedCertificate issuedCertificate() {
        if (state == CertificateState.PENDING || state == CertificateState.IN_USE) {
            return issuedCertificate;
        } else {
            return null;
        }
    }


    @Override
    public String toString() {
        return "CertificateStatus(" +
                "state=" + state +
                ", issuedCertificate=" + issuedCertificate +
                ", token=" + Arrays.toString(token) +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateStatus that = (CertificateStatus) o;
        return state == that.state &&
                Objects.equals(issuedCertificate, that.issuedCertificate) &&
                Arrays.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(state, issuedCertificate);
        result = 31 * result + Arrays.hashCode(token);
        return result;
    }
}
