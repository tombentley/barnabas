/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.PKIXReason;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.strimzi.operator.cluster.model.pki.PkiUtils.fingerprint;

/**
 * Does PKIX cert path validation for the current time ({@code now}) and {@code now + renewalPeriod}.
 */
public class Validator {
    public static class Result {

        public enum Type {
            /** Currently valid and valid at {@code now + renewalPeriod}. */
            OK,
            /** Not path to a trust anchor was found (i.e. not trusted). */
            NO_TRUST_ANCHOR,
            /** An expired certificate was found. */
            EXPIRED,
            /** Currently valid, but not valid at {@code now + renewalPeriod}. */
            WILL_EXPIRE,
            /** Some other kind of invalid. */
            OTHER
        }

        private final Result.Type type;
        private final X509Certificate expiring;
        private final TrustAnchor anchor;
        private final String description;

        public Result(Result.Type type, X509Certificate expiring, TrustAnchor anchor, String description) {
            this.type = type;
            this.expiring = expiring;
            this.anchor = anchor;
            this.description = description;
        }

        public Result.Type type() {
            return type;
        }

        public X509Certificate expiring() {
            return expiring;
        }

        public boolean isRootExpiring() {
            if (expiring != null && anchor != null) {
                return Objects.equals(expiring, anchor.getTrustedCert());
            }
            return false;
        }

        /**
         * @return When {@link #type()} is {@link Type#OK} or {@link Type#WILL_EXPIRE}, the trust anchor found,
         * otherwise null.
         */
        public TrustAnchor anchor() {
            return anchor;
        }

        /**
         * @return A description of the problem when {@link #type()} is {@link Type#OTHER}, otherwise null.*/
        public String description() {
            return description;
        }

        static Result ok(TrustAnchor anchor) {
            Objects.requireNonNull(anchor);
            return new Result(Result.Type.OK, null, anchor, null);
        }

        static Result noTrustAnchor() {
            return new Result(Result.Type.NO_TRUST_ANCHOR, null, null, null);
        }

        static Result expired(X509Certificate expiredCert) {
            return new Result(Result.Type.EXPIRED, expiredCert, null, null);
        }

        static Result willExpire(X509Certificate expiringCert, TrustAnchor anchor) {
            return new Result(Result.Type.WILL_EXPIRE, expiringCert, anchor, null);
        }

        static Result other(String description) {
            return new Result(Result.Type.OTHER, null, null, description);
        }

        @Override
        public String toString() {
            return "Result(" +
                    "type=" + type +
                    ", anchorFingerprint=" + (anchor == null || anchor.getTrustedCert() == null ? null : PkiUtils.fingerprint(anchor.getTrustedCert())) +
                    ", anchorSubject=" + (anchor == null ? null : anchor.getCA()) +
                    ", description='" + description + '\'' +
                    ')';
        }
    }

    private final Clock clock;
    private final int renewalDays;
    private final CertPathValidator pkixValidator;
    private final CertificateFactory certificateFactory;

    public Validator(Clock clock, int renewalDays) {
        this.clock = Objects.requireNonNull(clock);
        this.renewalDays = renewalDays;
        try {
            this.pkixValidator = CertPathValidator.getInstance("PKIX");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // standard part of Java: should never happen
        }

        try {
            this.certificateFactory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException(e); // standard part of Java: should never happen
        }
    }

    public Result validate(IssuedCertificate issued, Set<TrustAnchor> trustAnchors) {
        return validate(issued.completeChain(), trustAnchors);
    }

    public Result validate(List<X509Certificate> chain, Set<TrustAnchor> trustAnchors) {
        Instant now = clock.instant();
        return validate(now, pkixParameters(trustAnchors), chain);
    }

    private Result validate(Instant now,
                    PKIXParameters pkixParameters,
                    List<X509Certificate> chain) {
        Objects.requireNonNull(pkixParameters);

        try {
            // Validate whether there's currently a valid cert path
            CertPath certPath = certificateFactory.generateCertPath(chain);
            TrustAnchor currentTrustAnchor;
            try {
                currentTrustAnchor = pkixValidate(now, pkixParameters, certPath);
            } catch (CertPathValidatorException e) {
                CertPathValidatorException.Reason reason = e.getReason();
                if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                    return Result.expired(
                            (X509Certificate) e.getCertPath().getCertificates().get(e.getIndex()));
                } else if (reason == PKIXReason.NO_TRUST_ANCHOR) {
                    return Result.noTrustAnchor();
                } else {
                    return Result.other(describeCertPathFailure(e));
                }
            }

            // Validate whether there will still be a valid cert path in $renewalPeriod days time
            Instant date = now.plus(renewalDays, ChronoUnit.DAYS);
            try {
                TrustAnchor futureTrustAnchor = pkixValidate(date, pkixParameters, certPath);
                if (!currentTrustAnchor.equals(futureTrustAnchor)) {
                    throw new AssertionError();
                }
                return Result.ok(currentTrustAnchor);
            } catch (CertPathValidatorException e) {
                CertPathValidatorException.Reason reason = e.getReason();
                if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                    return Result.willExpire(
                            (X509Certificate) e.getCertPath().getCertificates().get(e.getIndex()),
                            currentTrustAnchor);
                } else if (reason == PKIXReason.NO_TRUST_ANCHOR) {
                    return Result.noTrustAnchor();
                } else {
                    return Result.other(describeCertPathFailure(e));
                }
            }
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    private PKIXParameters pkixParameters(Set<TrustAnchor> anchors) {
        PKIXParameters pkixParameters;
        try {
            pkixParameters = new PKIXParameters(anchors);
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
        pkixParameters.setRevocationEnabled(false);
        return pkixParameters;
    }

    private String describeCertPathFailure(CertPathValidatorException e) {
        int index = e.getIndex();
        if (index != -1) {
            X509Certificate certificate = (X509Certificate) e.getCertPath().getCertificates().get(index);
            return "The certificate at index " + index + " with fingerprint " + fingerprint(certificate) + " and subject " + certificate.getSubjectDN() + " has problem " + e.getReason();
        } else {
            return e.getReason().toString() + " (no certificate )";
        }
    }

    private TrustAnchor pkixValidate(Instant date, PKIXParameters pkixParameters, CertPath certPath)
            throws CertPathValidatorException {
        try {
            pkixParameters.setDate(new Date(date.getEpochSecond() * 1000L));
            return ((PKIXCertPathValidatorResult) pkixValidator.validate(certPath, pkixParameters)).getTrustAnchor();
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e); // should never happen
        }
    }
}
