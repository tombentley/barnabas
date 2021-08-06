/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.X509Certificate;
import java.time.Instant;
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
public class CertificateStatus<Id extends SubjectIdentity> {
    private final CertificateState state;
    private final Id subjectIdentity;
    private final IssuedCertificate issuedCertificate;
    private final byte[] token;
    private Fingerprint fingerprint = null;

    @JsonCreator
    public CertificateStatus(
            @JsonProperty("state") CertificateState state,
            @JsonProperty("subjectIdentity") Id subjectIdentity,
            @JsonProperty("requestToken") byte[] token,
            @JsonProperty("pending") IssuedCertificate pending) {
        this(state, subjectIdentity, token, pending, null);
    }

    CertificateStatus(CertificateState state, Id subjectIdentity, byte[] token, IssuedCertificate issuedCertificate, Fingerprint fingerprint) {
        this.state = Objects.requireNonNull(state);
        this.subjectIdentity = Objects.requireNonNull(subjectIdentity);
        this.issuedCertificate = issuedCertificate;
        this.token = token;
        this.fingerprint = fingerprint;
    }

    public static <Id extends SubjectIdentity> CertificateStatus<Id> createNew(Id subjectIdentity) {
        Objects.requireNonNull(subjectIdentity);
        return new CertificateStatus<>(CertificateState.NEW, subjectIdentity, null, null, null);
    }

    public CertificateStatus<Id> toRequested(byte[] token) {
        if (this.state != CertificateState.NEW && this.state != CertificateState.IN_USE)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.REQUESTED);
        return new CertificateStatus<>(CertificateState.REQUESTED, this.subjectIdentity, token, null, null);
    }

    public CertificateStatus<Id> toPending(IssuedCertificate issuedCertificate) {
        Objects.requireNonNull(issuedCertificate);
        if (this.state != CertificateState.REQUESTED)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.PENDING);
        return new CertificateStatus<>(CertificateState.PENDING, this.subjectIdentity, null, issuedCertificate, null);
    }

    public CertificateStatus<Id> toInUse() {
        if (this.state != CertificateState.PENDING)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.IN_USE);
        return new CertificateStatus<>(CertificateState.IN_USE, this.subjectIdentity, null, this.issuedCertificate, this.fingerprint);
    }

    public CertificateStatus<Id> toNotNeeded() {
        if (this.state != CertificateState.IN_USE)
            throw new AssertionError("Unexpected state transition: " + this.state + "->" + CertificateState.NOT_NEEDED);
        return new CertificateStatus<>(CertificateState.NOT_NEEDED, this.subjectIdentity, null, null, this.fingerprint);
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
    public Id subjectIdentity() {
        return subjectIdentity;
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
                ", subjectIdentity=" + subjectIdentity +
                ", issuedCertificate=" + issuedCertificate +
                ", token=" + Arrays.toString(token) +
                ", fingerprint=" + fingerprint +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateStatus<?> that = (CertificateStatus<?>) o;
        return state == that.state &&
                Objects.equals(subjectIdentity, that.subjectIdentity) &&
                Objects.equals(issuedCertificate, that.issuedCertificate) &&
                Arrays.equals(token, that.token) &&
                Objects.equals(fingerprint, that.fingerprint);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(state, subjectIdentity, issuedCertificate, fingerprint);
        result = 31 * result + Arrays.hashCode(token);
        return result;
    }
}
