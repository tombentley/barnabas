/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkIssued;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkNoop;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.WaitingForIssuance;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.WaitingForTrust;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

/**
 * Manages the issuance, validation and re-issuance of end-entity certificates using a {@link CertificateIssuer},
 * implementing all the transitions of the {@link CertificateStatus} state machine, and some of the transitions
 * of the {@link TrustState} state machine via a {@link UseManager}.
 *
 * Because a certificate might be issued with a new, untrusted, root CA certificate certificates cannot always
 * be used immediately: Trust in the new root CA certificate may have to be established first.
 * The caller of {@link #reconcile(RequiredCertificate, CertificateOutputWriter)} needs to inspect the result and
 * possibly wait for a {@link TrustManager} to establish trust in the trust anchor before deploying the
 * issued certificate for use.
 *
 * The state of issued certificates is made persistent (in Kube) via the {@link CertificateOutputWriter}, so that the
 * current state of each certificate's state machine is persisted across restarts of the operator process.
 */
public class CertificateController {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CertificateController.class);

    private final CertificateIssuer issuer;
    private final Validator validator;
    private final UseManager useManager;
    private final int requestedValidity;
    private final Reconciliation reconciliation;

    public CertificateController(Reconciliation reconciliation,
                                 //TrustPrivateState trustPrivateState,
                                 UseManager useManager,
                                 CertificateIssuer issuer,
                                 Validator validator, int requestedValidity) {
        this.useManager = useManager;
        this.issuer = Objects.requireNonNull(issuer);
        this.validator = Objects.requireNonNull(validator);
        this.requestedValidity = requestedValidity;
        this.reconciliation = reconciliation;
    }

    /**
     * <p>Reconcile the given requirement for an end-entity certificate.
     * Broadly speaking one of the following things may happen as a result:</p>
     * <ul>
     *     <li>No op, the certificate and chain already exists and is valid to no new certificates need to be issued.
     *         The certificate status starts, and remains, in {@link CertificateState#IN_USE}.
     *         {@link OkNoop} is returned.
     *         The caller doesn't need to do anything.</li>
     *
     *     <li>The end-entity certificate and/or one of the non-trusted CA certificates in the chain needs to be reissued.
     *         The certificate status starts in  {@link CertificateState#IN_USE}
     *         but the invalidity it detected so transitions to {@link CertificateState#REQUESTED}
     *         then to {@link CertificateState#IN_USE} (not to {@link CertificateState#PENDING}
     *         because the root is trusted already)
     *         There is no change in the trusted CA certificates, so {@link OkIssued}
     *         is returned.
     *         The caller will need to make the certificate available for use
     *         (e.g. by restarting the pods which use the end-entity certificate).</li>
     *
     *     <li>The root CA certificate needs to be (re-)issued.
     *         The certificate status starts in {@link CertificateState#NEW} or {@link CertificateState#IN_USE} but the invalidity it detected
     *         so transitions to {@link CertificateState#REQUESTED}
     *         then to {@link CertificateState#PENDING}
     *         Because there is a change in the trusted CA certificates,
     *         {@link WaitingForTrust} will be returned.
     *         The caller will need to ensure the new root CA certificate is trusted (e.g. by restarting the pods which will see the issued certificate as a client certificate)
     *         and then make the end-entity certificate available for use (e.g. by restarting the pods which use the end-entity certificate).</li>
     * </ul>
     * <p>The actual transitions can be more complicated than that. For instance, it might happen that issuance takes
     * a while, so {@link WaitingForIssuance} might be returned for some number of calls.</p>
     *
     * @param <I> The type of certificate identity.
     * @param requiredCertificate The required certificate.
     * @param writer The writer
     * @return The reconciliation result.
     * @throws CertificateException Cert ex
     */
    public <I extends SubjectIdentity> CertificateReconcileResult reconcile(
            RequiredCertificate<I> requiredCertificate,
            CertificateOutputWriter<I> writer) throws CertificateException {
        // TODO need a clean way to handle the NOT_NEEDED case.
        // Maybe that's a delete(SubjectIdentity) method.
        // Which would basically just to remove the state and the output certificates.

        CertificateStatus<I> certStatus = writer.loadStatus(requiredCertificate.subjectIdentity());
        final CertificateState startState = certStatus.state();
        LOGGER.debugCr(reconciliation, "Required cert {} has state {}", requiredCertificate.subjectIdentity(), startState);
        switch (startState) {
            case NEW: {
                certStatus = handleNew(certStatus, writer, requiredCertificate);
                // fall through
            }
            case REQUESTED: {
                return handleRequested(certStatus, requiredCertificate, writer);
            }
            case PENDING: {
                return handlePending(certStatus, requiredCertificate, writer);
            }
            case IN_USE: {
                return handleInUse(certStatus, requiredCertificate, writer);
            }
            case NOT_NEEDED:
                // This shouldn't occur here and only be handled at a higher level
                // TODO handle it
                throw new IllegalStateException();
        }
        throw new AssertionError();
    }

    private <I extends SubjectIdentity> CertificateStatus<I> handleNew(CertificateStatus<I> status,
                                        CertificateOutputWriter<I> writer, RequiredCertificate<I> requiredCertificate) {
        byte[] token = issuer.requestCertificate(requiredCertificate.subjectAndSan(), requestedValidity);
        LOGGER.debugCr(reconciliation, "Cert transition: identity: {}, to-state: {}", requiredCertificate.subjectIdentity(), CertificateState.REQUESTED);
        CertificateStatus<I> requested = status.toRequested(token);
        writer.storeStatus(requested);
        return requested;
    }

    private <I extends SubjectIdentity> CertificateReconcileResult handleRequested(CertificateStatus<I> status,
                                                       RequiredCertificate<I> requiredCertificate,
                                                       CertificateOutputWriter<I> writer)
            throws CertificateParsingException {

        // TODO it's possible that the certIssuer changes (e.g. strimzi to cert manager) after the request is made and before it completes
        // we need to be able to detect this and issue the request again
        var issuedOpt = issuer.pollForRequestedCertificate(status.tokenBytes());
        if (issuedOpt.isEmpty()) {
            // Not issued yet: Wait for another reconciliation
            return new WaitingForIssuance();
        }
        IssuedCertificate issued = issuedOpt.get();
        Subject subject = requiredCertificate.subjectAndSan();
        X509Certificate issuedCertificate = issued.issuedCertificate();
        X500Principal requestedPrincipal = subject.principal();
        X500Principal issuedPrincipal = issuedCertificate.getSubjectX500Principal();
        if (!issuedPrincipal.equals(requestedPrincipal)) {
            throw new IllegalStateException("Requested certificate for " + requestedPrincipal
                    + " but was issued a certificate for " + issuedCertificate.getSubjectDN());
        }
        Set<String> issuedSans = PkiUtils.dnsNames(issuedCertificate);
        Set<String> requestedSans = Set.copyOf(subject.subjectAltNames().values());
        if (!issuedSans.equals(requestedSans)) {
            throw new IllegalStateException("Requested certificate with subject alternative names " + requestedSans
                    + " but was issued a certificate with " + issuedSans);
        }

        LOGGER.debugCr(reconciliation, "Cert issued: identity: {}, fingerprint: {} rootFingerprint: {}", requiredCertificate.subjectIdentity(), issued.issuedFingerprint(), issued.rootCaFingerprint());
        TrustState trustState = useManager.state(issued.rootCaFingerprint());
        if (trustState == null) {
            // it's new, so we should add it in UNTRUSTED
            return waitingForTrust(status, writer, issued, issued.rootCaCertificate());
        } else {
            switch (trustState) {
                case UNTRUSTED:
                    return validateRequestedThatsNotYetTrustable(status, writer, requiredCertificate, issued, issued.rootCaCertificate());
                case TRUSTED_NEW:
                    return validateRequestedThatShouldBeTrusted(status, requiredCertificate, issued, issued.rootCaCertificate(), writer);
                case PHASE_OUT:
                    // fall through
                case ZOMBIE: {
                    // This shouldn't happen
                    LOGGER.warnCr(reconciliation,
                            "New certificate for {} was issued using a root CA certificate in state {}." +
                            "In theory this should never happen, so please report this as a bug.",
                            requiredCertificate.subjectIdentity(),
                            trustState);
                    status = handleNew(status, writer, requiredCertificate);
                    return new CertificateReconcileResult.IssuedBad();
                }
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private <I extends SubjectIdentity> CertificateReconcileResult validateRequestedThatShouldBeTrusted(CertificateStatus<I> status,
                                                                            RequiredCertificate<I> requiredCertificate,
                                                                            IssuedCertificate issued,
                                                                            X509Certificate trustRoot,
                                                                            CertificateOutputWriter<I> writer) {
        // If it's trusted then proceed to validate
        Set<TrustAnchor> trustAnchors = asTrustAnchor(trustRoot);
        Validator.Result result = validate(issued, trustAnchors);
        switch (result.type()) {
            case OK:
                // The issued certificate is signed by an already trusted CA cert
                LOGGER.debugCr(reconciliation, "Cert transition: identity: {}, to-state: {}", requiredCertificate.subjectIdentity(), CertificateState.PENDING);
                return deploy(status.toPending(issued), requiredCertificate, issued, trustRoot, writer);
            case WILL_EXPIRE: {
                LOGGER.warnCr(reconciliation,
                        "Newly issued certificate for {} is currently valid but will soon expire." +
                        "This suggests misconfiguration between the requested validity period of the certificate " +
                        "the actual validity period granted by the issuer, or the issuer using a " +
                        "CA certificate with a notEndDate within the renewalPeriod " +
                        "(misaligned issuer configuration and CR renewalPeriod)." +
                        " Will try to issue another end entity certificate for this subject.",
                        requiredCertificate.subjectIdentity());
                // fall through
            }
            case HAS_EXPIRED:
                // fall through
            case OTHER: {
                status = handleNew(status, writer, requiredCertificate);
                return new CertificateReconcileResult.IssuedBad();
            }
            case NO_TRUST_ANCHOR:
                // This should be impossible, it's in the trusted state according to the secret,
                // but not according to path validation
                throw new AssertionError();
            default:
                throw new IllegalStateException();
        }
    }

    private <I extends SubjectIdentity> CertificateReconcileResult validateRequestedThatsNotYetTrustable(CertificateStatus<I> status,
                                                                             CertificateOutputWriter<I> writer,
                                                                             RequiredCertificate<I> requiredCertificate,
                                                                             IssuedCertificate issued,
                                                                             X509Certificate trustRoot) {
        // We're still waiting
        Set<TrustAnchor> trustAnchors = asTrustAnchor(trustRoot);
        Validator.Result result = validate(issued, trustAnchors);
        switch (result.type()) {
            case WILL_EXPIRE:
                LOGGER.warnCr(reconciliation,
                        "The certificate issued for {} has entered its renewal period before trust " +
                        "was established for its root CA certificate", requiredCertificate.subjectIdentity());
                // fall through
            case OK:
                return waitingForTrust(status, writer, issued, trustRoot);
            case HAS_EXPIRED:
                LOGGER.errorCr(reconciliation,
                        "The certificate issued for {} has expired before trust " +
                        "was established for its root CA certificate", requiredCertificate.subjectIdentity());
                status = handleNew(status, writer, requiredCertificate);
                return new CertificateReconcileResult.IssuedBad();
            case OTHER:
                LOGGER.errorCr(reconciliation,
                        "The certificate issued for {} has some unexpected error {}, reissuing",
                        requiredCertificate.subjectIdentity(), result);
                status = handleNew(status, writer, requiredCertificate);
                return new CertificateReconcileResult.IssuedBad();
            case NO_TRUST_ANCHOR:
                // This should simply not happen
                throw new AssertionError();
            default:
                throw new IllegalStateException();

        }
    }

    private Set<TrustAnchor> asTrustAnchor(X509Certificate trustRoot) {
        return Set.of(new TrustAnchor(trustRoot, null));
    }

    private <I extends SubjectIdentity> CertificateReconcileResult handlePending(CertificateStatus<I> status,
                                                     RequiredCertificate<I> requiredCertificate,
                                                     CertificateOutputWriter<I> writer) {
        IssuedCertificate issued = status.issuedCertificate();
        Set<TrustAnchor> trustAnchors = trustAnchors(TrustState.TRUSTED_NEW);
        Validator.Result result = validate(issued, trustAnchors);
        switch (result.type()) { // should we query the trustState or should be pkixValidate?
            case OK:
            case WILL_EXPIRE: // it may be about to expire, but it's better than nothing
                // deploy it for use
                Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
//                TrustState trustState = useManager.state(fingerprint);
//                TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                return deploy(status, requiredCertificate, issued, result.anchor().getTrustedCert(), writer);
            case NO_TRUST_ANCHOR:
                // It's still not trusted yet: we have to wait
                return new WaitingForTrust(issued.rootCaCertificate());
            case HAS_EXPIRED:
            case OTHER:
                status = handleNew(status, writer, requiredCertificate);
                // TODO should loop, but return for now
                return new CertificateReconcileResult.PendingBad();
            default:
                throw new IllegalStateException();
        }
    }

    private <I extends SubjectIdentity> CertificateReconcileResult deploy(CertificateStatus<I> status,
                                              RequiredCertificate<I> requiredCertificate,
                                              IssuedCertificate issued,
                                              X509Certificate trustRoot,
                                              CertificateOutputWriter<I> writer) {
        LOGGER.debugCr(reconciliation, "Cert transition: identity: {}, to-state: {}", requiredCertificate.subjectIdentity(), CertificateState.IN_USE);
        status = status.toInUse();
        // store in output store before we change the status to in-use
        // this is so that we can't get into an unrecoverable state if the operator were to crash,
        // e.g. while writing the issued certificates.
        LOGGER.debugCr(reconciliation, "Writing output certificates: {}", requiredCertificate);

        try {
            // TODO This can add and replace certs, but what about removing old certs?
            writer.writeIssued(requiredCertificate.subjectIdentity(), issued);
        } catch (CertificateException | InvalidKeySpecException | KeyStoreException | IOException e) {
            throw new RuntimeException(e);
        }

        writer.storeStatus(status);
        return new OkIssued(trustRoot);
    }

    private <I extends SubjectIdentity> CertificateReconcileResult handleInUse(
            CertificateStatus<I> status,
            RequiredCertificate<I> requiredCertificate,
            CertificateOutputWriter<I> writer) throws CertificateParsingException {
        // Is it still OK? validate it to find out
        IssuedCertificate issued = status.issuedCertificate(); // Alternative load the chain from the p12
        Set<TrustAnchor> trustAnchors = trustAnchors(TrustState.TRUSTED_NEW, TrustState.PHASE_OUT);
        Validator.Result result = validate(issued, trustAnchors);
        switch (result.type()) {
            case OK: {
                // still valid: do nothing (this is the common case once a cluster is bootstrapped)
                Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
                useManager.state(fingerprint);
//                TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                return new OkNoop(result.anchor().getTrustedCert());
            }
            case OTHER:
            case NO_TRUST_ANCHOR:
            case HAS_EXPIRED:
                LOGGER.errorCr(reconciliation, "Oh dear we shouldn't normally get here");
                // fall thru
            case WILL_EXPIRE: {
                status = handleNew(status, writer, requiredCertificate);
                if (result.isRootExpiring()) {
                    Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
                    //TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                    LOGGER.debugCr(reconciliation, "Trust transition: root: {}, to-state: {}", fingerprint, useManager.state(fingerprint));
                    useManager.depend(result.anchor().getTrustedCert());
                    //trustPrivateState.transition(Map.of(fingerprint, trustRoot));
                }
                //return CertificateReconcileResult.waitingForIssuance();
                return handleRequested(status, requiredCertificate, writer);
            }
            default:
                throw new IllegalStateException();
        }
    }

    private Validator.Result validate(IssuedCertificate issued, Set<TrustAnchor> trustAnchors) {
        Validator.Result result = validator.validate(issued, trustAnchors);
        LOGGER.debugCr(reconciliation, "Cert validation: cert: {}, anchors: {}, result: {}",
                issued.issuedFingerprint(),
                trustAnchors.stream().map(a -> PkiUtils.fingerprint(a.getTrustedCert())).collect(Collectors.toSet()),
                result.type());
        return result;
    }

    private <I extends SubjectIdentity> CertificateReconcileResult waitingForTrust(CertificateStatus<I> status,
                                                       CertificateOutputWriter<I> writer,
                                                       IssuedCertificate issued,
                                                       X509Certificate trustRoot) {
        LOGGER.debugCr(reconciliation, "Cert transition: identity: {}, to-state: {}",
                issued.issuedFingerprint(), CertificateState.PENDING);
        status = status.toPending(issued);
        writer.storeStatus(status);
        useManager.requireTrust(trustRoot);
        //trustPrivateState.transition(Map.of(untrustedTrustRoot.fingerprint(), untrustedTrustRoot));
        return new WaitingForTrust(trustRoot);
    }

    private Set<TrustAnchor> trustAnchors(TrustState... s) {
        return useManager.certsInStates(Set.of(s)).stream()
                .map(cert -> new TrustAnchor(cert, null))
                .collect(Collectors.toSet());
    }

}
