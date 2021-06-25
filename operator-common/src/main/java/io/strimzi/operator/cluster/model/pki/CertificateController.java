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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkIssued;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.OkNoop;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.WaitingForIssuance;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult.WaitingForTrust;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages the issuance, validation and re-issuance of end-entity certificates using a {@link CertificateIssuer},
 * implementing all the transitions of the {@link CertificateStatus} state machine, and some of the transitions
 * of the {@link TrustState} state machine.
 *
 * Because a certificate might be issued with a new, untrusted, root CA certificate certificates cannot always
 * be used immediately: Trust in the new root CA certificate may have to be established first.
 * The caller of {@link #reconcile(RequiredCertificate)} needs to inspect the result and possibly establish trust
 * in the trust anchor before deploying the issued certificate for use.
 *
 * The state of issued certificates is made persistent (in Kube) via the {@link CertificateStatusStore}, so that the
 * current state of each certificate's state machine is persisted across restarts of the operator process.
 *
 * The trustedness of CA certificates is determined by the {@link TrustPrivateState}.
 */
public class CertificateController {

    private static final Logger LOGGER = LogManager.getLogger(CertificateController.class);

    private final Validator validator;
    private final TrustPrivateState trustPrivateState;
    private final CertificateStatusStore certificateStatusStore;
    private final List<CertificateOutputWriter> certificateOutputWriters;
    private final CertificateIssuer issuer;
    private final int requestedValidity;


    public CertificateController(TrustPrivateState trustPrivateState,
                                 CertificateStatusStore certificateStatusStore,
                                 List<CertificateOutputWriter> certificateOutputWriters,
                                 CertificateIssuer issuer,
                                 Validator validator, int requestedValidity) {
        // TODO we're just passing in the issuer, but really that has its own little state machine
        // or at least some state mgmt for knowing when we need to regenerate???
        // Or is it a recursive use of the trust controller and certificate controller?
        this.trustPrivateState = Objects.requireNonNull(trustPrivateState);
        this.certificateStatusStore = Objects.requireNonNull(certificateStatusStore);
        this.certificateOutputWriters = Objects.requireNonNull(certificateOutputWriters);
        this.issuer = Objects.requireNonNull(issuer);
        this.validator = Objects.requireNonNull(validator);
        this.requestedValidity = Objects.requireNonNull(requestedValidity);
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
     * @param requiredCertificate The required certificate.
     * @return The reconciliation result.
     * @throws CertificateException Cert ex
     */
    public CertificateReconcileResult reconcile(RequiredCertificate requiredCertificate) throws CertificateException {
        // TODO need a clean way to handle the NOT_NEEDED case.
        // Maybe that's a delete(SubjectIdentity) method.
        // Which would basically just to remove the state and the output certificates.

        Map<Fingerprint, TrustRoot> trustState = this.trustPrivateState.load();
        CertificateStatus certStatus = certificateStatusStore.loadStatus(requiredCertificate.subjectIdentity());
        final CertificateState startState = certStatus.state();
        switch (startState) {
            case NEW: {
                certStatus = handleNew(certStatus, requiredCertificate);
                // fall through
            }
            case REQUESTED: {
                return handleRequested(certStatus, requiredCertificate, trustState);
            }
            case PENDING: {
                return handlePending(certStatus, requiredCertificate, trustState);
            }
            case IN_USE: {
                return handleInUse(certStatus, requiredCertificate, trustState);
            }
            case NOT_NEEDED:
                // This shouldn't occur here and only be handled at a higher level
                // TODO handle it
                throw new IllegalStateException();
        }
        throw new AssertionError();
    }

    private CertificateStatus handleNew(CertificateStatus status, RequiredCertificate requiredCertificate) {
        byte[] token = issuer.requestCertificate(requiredCertificate.subjectAndSan(), requestedValidity);
        CertificateStatus requested = status.toRequested(token);
        certificateStatusStore.storeStatus(requested);
        return requested;
    }

    private CertificateReconcileResult handleRequested(CertificateStatus status,
                                               RequiredCertificate requiredCertificate,
                                               Map<Fingerprint, TrustRoot> trustState)
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

        TrustRoot trustRoot = trustState.get(PkiUtils.fingerprint(issued.rootCaCertificate()));
        if (trustRoot == null) {
            // it's new, so we should add it in UNTRUSTED
            return waitingForTrust(status, issued, TrustRoot.untrusted(issued.rootCaCertificate()));
        } else {
            switch (trustRoot.state()) {
                case UNTRUSTED:
                    return validateRequestedThatsNotYetTrustable(status, requiredCertificate, issued, trustRoot);
                case TRUSTED:
                    return validateRequestedThatShouldBeTrusted(status, requiredCertificate, issued, trustRoot);
                case PHASE_OUT:
                    // fall through
                case ZOMBIE: {
                    // This shouldn't happen
                    LOGGER.warn("New certificate for {} was issued using a root CA certificate in state {}." +
                                    "In theory this should never happen, so please report this as a bug.",
                            requiredCertificate.subjectIdentity(),
                            trustRoot.state());
                    status = handleNew(status, requiredCertificate);
                    return new CertificateReconcileResult.IssuedBad();
                }
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private CertificateReconcileResult validateRequestedThatShouldBeTrusted(CertificateStatus status,
                                                                            RequiredCertificate requiredCertificate,
                                                                            IssuedCertificate issued,
                                                                            TrustRoot trustRoot) {
        // If it's trusted then proceed to validate
        Validator.Result result = validator.validate(issued, asTrustAnchor(trustRoot));
        switch (result.type()) {
            case OK:
                // The issued certificate is signed by an already trusted CA cert
                return deploy(status.toPending(issued), requiredCertificate, issued, trustRoot);
            case WILL_EXPIRE: {
                LOGGER.warn("Newly issued certificate for {} is currently valid but will soon expire." +
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
                status = handleNew(status, requiredCertificate);
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

    private CertificateReconcileResult validateRequestedThatsNotYetTrustable(CertificateStatus status,
                                                                             RequiredCertificate requiredCertificate,
                                                                             IssuedCertificate issued,
                                                                             TrustRoot trustRoot) {
        // We're still waiting
        Validator.Result result = validator.validate(issued, asTrustAnchor(trustRoot));
        switch (result.type()) {
            case WILL_EXPIRE:
                LOGGER.warn("The certificate issued for {} has entered its renewal period before trust " +
                        "was established for its root CA certificate", requiredCertificate.subjectIdentity());
                // fall through
            case OK:
                return waitingForTrust(status, issued, trustRoot);
            case HAS_EXPIRED:
                LOGGER.error("The certificate issued for {} has expired before trust " +
                        "was established for its root CA certificate", requiredCertificate.subjectIdentity());
                status = handleNew(status, requiredCertificate);
                return new CertificateReconcileResult.IssuedBad();
            case OTHER:
                LOGGER.error("The certificate issued for {} has some unexpected error {}, reissuing",
                        requiredCertificate.subjectIdentity(), result);
                status = handleNew(status, requiredCertificate);
                return new CertificateReconcileResult.IssuedBad();
            case NO_TRUST_ANCHOR:
                // This should simply not happen
                throw new AssertionError();
            default:
                throw new IllegalStateException();

        }
    }

    private Set<TrustAnchor> asTrustAnchor(TrustRoot trustRoot) {
        return Set.of(new TrustAnchor(trustRoot.x509(), null));
    }

    private CertificateReconcileResult handlePending(CertificateStatus status,
                                             RequiredCertificate requiredCertificate,
                                             Map<Fingerprint, TrustRoot> trustState) {
        IssuedCertificate issued = status.issuedCertificate();
        Validator.Result result = validator.validate(issued, trustAnchors(trustState, TrustState.TRUSTED));
        switch (result.type()) { // should we query the trustState or should be pkixValidate?
            case OK:
            case WILL_EXPIRE: // it may be about to expire, but it's better than nothing
                // deploy it for use
                Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
                TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                return deploy(status, requiredCertificate, issued, trustRoot);
            case NO_TRUST_ANCHOR:
                // It's still not trusted yet: we have to wait
                return new WaitingForTrust(TrustRoot.untrusted(issued.rootCaCertificate()));
            case HAS_EXPIRED:
            case OTHER:
                status = handleNew(status, requiredCertificate);
                // TODO should loop, but return for now
                return new CertificateReconcileResult.PendingBad();
            default:
                throw new IllegalStateException();
        }
    }

    private CertificateReconcileResult deploy(CertificateStatus status,
                                              RequiredCertificate requiredCertificate,
                                              IssuedCertificate issued, TrustRoot trustRoot) {
        status = status.toInUse();
        // store in output store before we change the status to in-use
        // this is so that we can't get into an unrecoverable state if the operator were to crash,
        // e.g. while writing the issued certificates.
        for (var writer : certificateOutputWriters) {
            try {
                // TODO This can add and replace certs, but what about removing old certs?
                writer.writeIssued(requiredCertificate.subjectIdentity(), issued);
            } catch (CertificateException | InvalidKeySpecException | KeyStoreException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        certificateStatusStore.storeStatus(status);
        return new OkIssued(trustRoot);
    }

    private CertificateReconcileResult handleInUse(CertificateStatus status,
                                           RequiredCertificate requiredCertificate,
                                           Map<Fingerprint, TrustRoot> trustState) throws CertificateParsingException {
        // Is it still OK? validate it to find out
        IssuedCertificate issued = status.issuedCertificate(); // Alternative load the chain from the p12
        Validator.Result result = validator.validate(issued, trustAnchors(trustState, TrustState.TRUSTED, TrustState.PHASE_OUT));
        switch (result.type()) {
            case OK: {
                // still valid: do nothing (this is the common case once a cluster is bootstrapped)
                Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
                TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                return new OkNoop(trustRoot);
            }
            case OTHER:
            case NO_TRUST_ANCHOR:
            case HAS_EXPIRED:
                LOGGER.error("Oh dear we shouldn't normally get here");
                // fall thru
            case WILL_EXPIRE: {
                status = handleNew(status, requiredCertificate);
                if (result.isRootExpiring()) {
                    Fingerprint fingerprint = PkiUtils.fingerprint(result.anchor().getTrustedCert());
                    TrustRoot trustRoot = trustState.get(fingerprint).toPhaseOut();
                    trustPrivateState.transition(Map.of(fingerprint, trustRoot));
                }
                //return CertificateReconcileResult.waitingForIssuance();
                return handleRequested(status, requiredCertificate, trustState);
            }
            default:
                throw new IllegalStateException();
        }
    }

    private CertificateReconcileResult waitingForTrust(CertificateStatus status,
                                                       IssuedCertificate issued,
                                                       TrustRoot untrustedTrustRoot) {
        status = status.toPending(issued);
        certificateStatusStore.storeStatus(status);
        trustPrivateState.transition(Map.of(untrustedTrustRoot.fingerprint(), untrustedTrustRoot));
        return new WaitingForTrust(untrustedTrustRoot);
    }

    private Set<TrustAnchor> trustAnchors(Map<Fingerprint, TrustRoot> trustState, TrustState... s) {
        Set<TrustState> x = Set.of(s);
        return trustState.values().stream()
            .filter(root -> x.contains(root.state()))
            .map(root -> new TrustAnchor(root.x509(), null))
            .collect(Collectors.toSet());
    }

}
