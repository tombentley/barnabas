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

public class CertificateStatus {
    private final CertificateState state;
    private final SubjectIdentity subjectIdentity;
    private final IssuedCertificate issuedCertificate;
    private final byte[] token;
    private Fingerprint fingerprint = null;

    @JsonCreator
    private CertificateStatus(CertificateState state, SubjectIdentity subjectIdentity, byte[] token) {
        this(state, subjectIdentity, token, null, null);
    }

    private CertificateStatus(CertificateState state, SubjectIdentity subjectIdentity, byte[] token, IssuedCertificate issuedCertificate, Fingerprint fingerprint) {
        this.state = state;
        this.subjectIdentity = subjectIdentity;
        this.issuedCertificate = issuedCertificate;
        this.token = token;
        this.fingerprint = fingerprint;
    }

    public static CertificateStatus createNew(SubjectIdentity subjectIdentity) {
        return new CertificateStatus(CertificateState.NEW, subjectIdentity, null, null, null);
    }

    public CertificateStatus toRequested(byte[] token) {
        assert (this.state == CertificateState.NEW);
        return new CertificateStatus(CertificateState.REQUESTED, this.subjectIdentity, token, null, null);
    }

    public CertificateStatus toPending(IssuedCertificate issuedCertificate) {
        assert (this.state == CertificateState.REQUESTED);
        return new CertificateStatus(CertificateState.PENDING, this.subjectIdentity, null, issuedCertificate, null);
    }

    public CertificateStatus toInUse() {
        assert (this.state == CertificateState.PENDING);
        return new CertificateStatus(CertificateState.IN_USE, this.subjectIdentity, null, this.issuedCertificate, this.fingerprint);
    }

    public CertificateStatus toNotNeeded() {
        assert (this.state == CertificateState.IN_USE);
        return new CertificateStatus(CertificateState.NOT_NEEDED, this.subjectIdentity, null, null, this.fingerprint);
    }

    public byte[] token() {
        if (state == CertificateState.REQUESTED) {
            return token;
        } else {
            throw new IllegalStateException(toString());
        }
    }

    public Fingerprint fingerprint() {
        return fingerprint == null ? fingerprint = PkiUtils.fingerprint(x509()) : fingerprint;
    }

    public SubjectIdentity subjectIdentity() {
        return subjectIdentity;
    }

    X509Certificate x509() {
        return issuedCertificate().issuedCertificate();
    }

    public CertificateState state() {
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
        CertificateStatus that = (CertificateStatus) o;
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

    public IssuedCertificate issuedCertificate() {
        if (state == CertificateState.PENDING || state == CertificateState.IN_USE) {
            return issuedCertificate;
        } else {
            throw new IllegalStateException(toString());
        }
    }
}
