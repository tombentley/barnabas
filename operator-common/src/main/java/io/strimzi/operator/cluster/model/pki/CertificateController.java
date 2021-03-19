/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import javax.security.auth.x500.X500Principal;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.operator.cluster.model.pki.PkiUtils.fingerprint;

public class CertificateController {
    private static final Logger LOGGER = LogManager.getLogger(CertificateController.class);
    private final Clock clock;
    private final CertPathValidator pkixValidator;
    private final TrustController.PrivateState trustState;
    private final CertificateStatusStore certificateStatusStore;
    private final List<CertificateOutputWriter> certificateOutputWriters;
    private final CertificateFactory certificateFactory;
    private final CertificateIssuer issuer;
    private final int renewalDays;

    public CertificateController(Clock clock, TrustController.PrivateState trustState,
                                 CertificateStatusStore certificateStatusStore,
                                 List<CertificateOutputWriter> certificateOutputWriters,
                                 CertificateIssuer issuer, int renewalDays) {
        this.clock = clock;
        this.trustState = trustState;
        this.certificateStatusStore = certificateStatusStore;
        this.certificateOutputWriters = certificateOutputWriters;
        this.issuer = issuer;
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

    public boolean reconcile(RequiredCertificate requiredCertificate) throws CertificateException {
        Instant now = clock.instant();
        Map<Fingerprint, TrustRoot> trustState = this.trustState.load();
        CertificateStatus status = certificateStatusStore.load(requiredCertificate.subjectIdentity());
        final CertificateState startState = status.state();
        switch (startState) {
            case NEW: {
                status = requestCertificate(status, requiredCertificate);
                // fall through
            }
            case REQUESTED: {
                // TODO could optimise away the load if fallen through
                var issuedOpt = issuer.pollForRequestedCertificate(status.token());
                if (issuedOpt.isEmpty()) {
                    // Not issued yet: Wait for another reconciliation
                    return startState != status.state();
                }
                IssuedCertificate issued = issuedOpt.get();
                try {
                    validateIssuedCertificate(requiredCertificate.subjectAndSan(), issued);
                } catch (Exception e) {
                    // ???
                }

                // fall through

                Result result = validate(now, pkixParameters(trustAnchors(trustState, TrustState.TRUSTED)), null, issued.completeChain());
                switch (result.type) {
                    case OK:
                        status = status.toInUse();
                        // store in output store
                        for (var w : certificateOutputWriters) {
                            w.write(issued);
                        }
                        certificateStatusStore.store(requiredCertificate.subjectIdentity(), status);
                        return true;
                    case WILL_EXPIRE:
                    case EXPIRED:
                    case OTHER:
                        status = requestCertificate(status, requiredCertificate);
                        // TODO should loop, but return for now
                        return false;
                    case NO_TRUST_ANCHOR:
                        // store in pending store
                        status = status.toPending(issued);
                        certificateStatusStore.store(requiredCertificate.subjectIdentity(), status);
                        return false;
                    default:
                        throw new IllegalStateException();
                }
            }
            case PENDING: {
                IssuedCertificate issued = status.issuedCertificate();
                Result result = validate(now, pkixParameters(trustAnchors(trustState, TrustState.TRUSTED)), null, issued.completeChain());
                switch (result.type) {// should we query the trustState or should be pkixValidate?
                    case OK:
                    case WILL_EXPIRE: // it may be about to expire, but it's better than nothing
                        // deploy it for use
                        status = status.toInUse();
                        certificateStatusStore.store(requiredCertificate.subjectIdentity(), status);
                        return true;
                    case NO_TRUST_ANCHOR:
                        // It's not trusted yet: we have to wait
                        return false;
                    case EXPIRED:
                    case OTHER:
                        status = requestCertificate(status, requiredCertificate);
                        // TODO should loop, but return for now
                        return false;
                    default:
                        throw new IllegalStateException();
                }
            }
            case IN_USE: {
                // Is it still OK? validate it to find out
                IssuedCertificate issued = status.issuedCertificate(); // Alternative load the chain from the p12
                Result result = validate(now, pkixParameters(trustAnchors(trustState, TrustState.TRUSTED, TrustState.PHASE_OUT)), null, issued.completeChain());
                switch (result.type) {
                    case OK:
                        // still valid: do nothing (this is the common case)
                        return false;
                    case OTHER:
                    case NO_TRUST_ANCHOR:
                    case EXPIRED:
                        LOGGER.error("Oh dear we shouldn't normally get here");
                        // fall thru
                    case WILL_EXPIRE:
                        status = requestCertificate(status, requiredCertificate);
                        return true;
                    default:
                        throw new IllegalStateException();
                }
            }
            case NOT_NEEDED:
                // This shouldn't occur here and only be handled at a higher level
                // TODO handle it
                throw new IllegalStateException();
        }
        throw new AssertionError();
    }

    CertificateStatus requestCertificate(CertificateStatus status, RequiredCertificate requiredCertificate) {
        byte[] token = issuer.requestCertificate(requiredCertificate.subjectAndSan(), 10);
        CertificateStatus requested = status.toRequested(token);
        certificateStatusStore.store(requiredCertificate.subjectIdentity(), requested);
        return requested;
    }

    Set<TrustAnchor> trustAnchors(Map<Fingerprint, TrustRoot> trustState, TrustState... s) {
        Set<TrustState> x = Set.of(s);
        return trustState.values().stream()
            .filter(root -> x.contains(root.state()))
            .map(root -> new TrustAnchor(root.x509(), null))
            .collect(Collectors.toSet());
    }


    /** Validate a newly issued certificate */
    void validateIssuedCertificate(Subject subject, IssuedCertificate issuedCert) throws CertificateParsingException {
        X509Certificate issuedCertificate = issuedCert.issuedCertificate();
        if (!issuedCertificate.getSubjectX500Principal().equals(new X500Principal("CN=" + subject.commonName() + ", O=" + subject.organizationName()))) {
            throw new IllegalStateException("Requested certificate for " + subject.opensslDn() + " but was issued a certificate for " + issuedCertificate.getSubjectDN());
        }
        if (issuedCertificate.getSubjectAlternativeNames().equals(subject.subjectAltNames().values())) {
            throw new IllegalStateException("Requested certificate with subject alternative names" + subject.subjectAltNames().values() + " but was issued a certificate with " + issuedCertificate.getSubjectAlternativeNames());
        }
        // TODO do cert path validation too (with UNTRUSTED+TRUSTED certs as trust roots)
        // That way we can verify that new certs are actually fix problems
        // E.g. what if the problem is in an intermediate certificate? It's possible that a newly issued cert has the same problem
        // Detecting it here allows use to fail the reconciliation, rather than loop pointlessly
        // creating new invalid certificates.
    }


    static class Result {
        enum Type {
            OK,
            NO_TRUST_ANCHOR,
            EXPIRED,
            WILL_EXPIRE,
            OTHER
        }
        private final Type type;
        private final TrustAnchor anchor;
        private final String description;

        public Result(Type type, TrustAnchor anchor, String description) {
            this.type = type;
            this.anchor = anchor;
            this.description = description;
        }

        static Result ok(TrustAnchor anchor) {
            Objects.requireNonNull(anchor);
            return new Result(Type.OK, anchor, null);
        }
        static Result noTrustAnchor() {
            return new Result(Type.NO_TRUST_ANCHOR, null, null);
        }
        static Result expired() {
            return new Result(Type.EXPIRED, null, null);
        }
        static Result willExpire(TrustAnchor anchor) {
            return new Result(Type.WILL_EXPIRE, anchor, null);
        }
        static Result other(String description) {
            return new Result(Type.OTHER, null, description);
        }
    }

    Result validate(Instant now,
                    PKIXParameters pkixParameters,
                    RequiredCertificate requiredCertificate,
                    List<X509Certificate> chain) {
        Objects.requireNonNull(pkixParameters);

        try {
            // Validate whether there's currently a valid cert path
            CertPath certPath = certificateFactory.generateCertPath(chain);
            TrustAnchor currentTrustAnchor;
            try {
                currentTrustAnchor = pkixValidate(now, pkixParameters, requiredCertificate, certPath);
            } catch (CertPathValidatorException e) {
                CertPathValidatorException.Reason reason = e.getReason();
                if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                    return Result.expired();
                } else if (reason == PKIXReason.NO_TRUST_ANCHOR) {
                    return Result.noTrustAnchor();
                } else {
                    return Result.other(describeCertPathFailure(e));
                }
            }

            // Validate whether there will still be a valid cert path in $renewalPeriod days time
            Instant date = now.plus(renewalDays, ChronoUnit.DAYS);
            try {
                TrustAnchor futureTrustAnchor = pkixValidate(date, pkixParameters, requiredCertificate, certPath);
                if (!currentTrustAnchor.equals(futureTrustAnchor)) {
                    throw new AssertionError();
                }
                return Result.ok(currentTrustAnchor);
            } catch (CertPathValidatorException e) {
                CertPathValidatorException.Reason reason = e.getReason();
                if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                    return Result.willExpire(currentTrustAnchor);
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

    private String describeCertPathFailure(CertPathValidatorException e) {
        int index = e.getIndex();
        if (index != -1) {
            X509Certificate certificate = (X509Certificate) e.getCertPath().getCertificates().get(index);
            return "The certificate at index " + index + " with fingerprint " + fingerprint(certificate) + " and subject " + certificate.getSubjectDN() + " has problem " + e.getReason();
        } else {
            return e.getReason().toString() + " (no certificate )";
        }
    }

    TrustAnchor pkixValidate(Instant date, PKIXParameters pkixParameters, RequiredCertificate requiredCertificate, CertPath certPath)
            throws CertPathValidatorException {
        try {
            pkixParameters.setDate(new Date(date.getEpochSecond() * 1000L));
            TrustAnchor trustAnchor = ((PKIXCertPathValidatorResult) pkixValidator.validate(certPath, pkixParameters)).getTrustAnchor();
            LOGGER.debug("Valid cert path exists at {}: cert: {}, trustAnchor: {}", date, requiredCertificate, trustAnchor);
            return trustAnchor;
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e); // should never happen
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
}
