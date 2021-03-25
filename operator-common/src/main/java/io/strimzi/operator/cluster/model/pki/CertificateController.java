/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateEncodingException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public CertificateReconcileResult reconcile(RequiredCertificate requiredCertificate) throws CertificateException {
        // TODO need a clean way to handle the NOT_NEEDED case.
        // Maybe that's a delete(SubjectIdentity) method.
        // Which would basically just to remove the state and the output certificates.

        Map<Fingerprint, TrustRoot> trustState = this.trustPrivateState.load();
        CertificateStatus status = certificateStatusStore.loadStatus(requiredCertificate.subjectIdentity());
        final CertificateState startState = status.state();
        switch (startState) {
            case NEW: {
                status = handleNew(status, requiredCertificate);
                // fall through
            }
            case REQUESTED: {
                return handleRequested(status, requiredCertificate, trustState);
            }
            case PENDING: {
                return handlePending(status, requiredCertificate, trustState);
            }
            case IN_USE: {
                return handleInUse(status, requiredCertificate, trustState);
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
            throws CertificateParsingException, CertificateEncodingException {

        // TODO could optimise away the load if fallen through
        // TODO it's possible that the certIssuer changes (e.g. strimzi to cert manager) after the request is made and before it completes
        // we need to be able to detect this and issue the request again
        var issuedOpt = issuer.pollForRequestedCertificate(status.tokenBytes());
        if (issuedOpt.isEmpty()) {
            // Not issued yet: Wait for another reconciliation
            return CertificateReconcileResult.waitingForIssuance();
        }
        IssuedCertificate issued = issuedOpt.get();
        Subject subject = requiredCertificate.subjectAndSan();
        X509Certificate issuedCertificate = issued.issuedCertificate();
        X500Principal requestedPrincipal = subject.principal();
        X500Principal issuedPrincipal = issuedCertificate.getSubjectX500Principal();
        if (!issuedPrincipal.equals(requestedPrincipal)) {
            throw new IllegalStateException("Requested certificate for " + requestedPrincipal + " but was issued a certificate for " + issuedCertificate.getSubjectDN());
        }
        Set<String> issuedSans = PkiUtils.dnsNames(issuedCertificate);
        Set<String> requestedSans = Set.copyOf(subject.subjectAltNames().values());
        if (!issuedSans.equals(requestedSans)) {
            throw new IllegalStateException("Requested certificate with subject alternative names " + requestedSans + " but was issued a certificate with " + issuedSans);
        }

        Set<TrustAnchor> anchors = trustAnchors(trustState, TrustState.TRUSTED);
        if (anchors.isEmpty()) {
            return waitingForTrust(requiredCertificate, status, issued);
        }
        Validator.Result result = validator.validate(issued, anchors);
        switch (result.type()) {
            case OK:
                // The issued certificate is signed by an already trusted CA cert
                return deploy(status, requiredCertificate, issued);
            case WILL_EXPIRE:
            case EXPIRED:
            case OTHER: // TODO OTHER is probably an error case => reconciliation error.
                status = handleNew(status, requiredCertificate);
                // TODO should loop, but return for now
                // TODO can we detect the case where the issuer issued a cert which isn't valid?
                return CertificateReconcileResult.issuedBad();
            case NO_TRUST_ANCHOR:
                // TODO we can validate again with the issued cert's root CA cert as the trust anchor
                // validate that the issued certificate is good. If not => reconciliation error.
                status = status.toPending(issued);
                // store in pending store
                return waitingForTrust(requiredCertificate, status, issued);
            default:
                throw new IllegalStateException();
        }
    }

    private CertificateReconcileResult handlePending(CertificateStatus status,
                                             RequiredCertificate requiredCertificate,
                                             Map<Fingerprint, TrustRoot> trustState) {
        IssuedCertificate issued = status.issuedCertificate();
        Validator.Result result = validator.validate(issued, trustAnchors(trustState, TrustState.TRUSTED));
        switch (result.type()) {// should we query the trustState or should be pkixValidate?
            case OK:
            case WILL_EXPIRE: // it may be about to expire, but it's better than nothing
                // deploy it for use
                return deploy(status, requiredCertificate, issued);
            case NO_TRUST_ANCHOR:
                // It's still not trusted yet: we have to wait
                return CertificateReconcileResult.waitingForTrust(new TrustRoot(issued.rootCaCertificate(), TrustState.TRUSTED));
            case EXPIRED:
            case OTHER:
                status = handleNew(status, requiredCertificate);
                // TODO should loop, but return for now
                return CertificateReconcileResult.pendingBad();
            default:
                throw new IllegalStateException();
        }
    }

    private CertificateReconcileResult deploy(CertificateStatus status, RequiredCertificate requiredCertificate, IssuedCertificate issued) {
        status = status.toInUse();
        // store in output store
        for (var writer : certificateOutputWriters) {
            try {
                // TODO This can add and replace certs, but what about removing old certs?
                writer.writeIssued(requiredCertificate.subjectIdentity(), issued);
            } catch (CertificateException | InvalidKeySpecException | KeyStoreException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        certificateStatusStore.storeStatus(status);
        return CertificateReconcileResult.ok();
    }

    private CertificateReconcileResult handleInUse(CertificateStatus status,
                                           RequiredCertificate requiredCertificate,
                                           Map<Fingerprint, TrustRoot> trustState) {
        // Is it still OK? validate it to find out
        IssuedCertificate issued = status.issuedCertificate(); // Alternative load the chain from the p12
        Validator.Result result = validator.validate(issued, trustAnchors(trustState, TrustState.TRUSTED, TrustState.PHASE_OUT));
        switch (result.type()) {
            case OK:
                // still valid: do nothing (this is the common case once a cluster is bootstrapped)
                return CertificateReconcileResult.ok();
            case OTHER:
            case NO_TRUST_ANCHOR:
            case EXPIRED:
                LOGGER.error("Oh dear we shouldn't normally get here");
                // fall thru
            case WILL_EXPIRE:
                status = handleNew(status, requiredCertificate);
                return CertificateReconcileResult.waitingForIssuance();
            default:
                throw new IllegalStateException();
        }
    }

    private CertificateReconcileResult waitingForTrust(RequiredCertificate requiredCertificate, CertificateStatus status, IssuedCertificate issued) {
        status = status.toPending(issued);
        certificateStatusStore.storeStatus(status);
        return CertificateReconcileResult.waitingForTrust(new TrustRoot(issued.rootCaCertificate(), TrustState.TRUSTED));
    }


    private Set<TrustAnchor> trustAnchors(Map<Fingerprint, TrustRoot> trustState, TrustState... s) {
        Set<TrustState> x = Set.of(s);
        return trustState.values().stream()
            .filter(root -> x.contains(root.state()))
            .map(root -> new TrustAnchor(root.x509(), null))
            .collect(Collectors.toSet());
    }





}
