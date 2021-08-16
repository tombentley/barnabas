/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.spec.InvalidKeySpecException;
import java.util.Optional;
import java.util.Set;

import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CertificateControllerTest extends OpensslCaTestHarness {

    private final TestId subjectIdentity = new TestId("foo");
    private final Subject subject = new Subject.Builder().withCommonName("My Entity Cert").build();
    private final int requestedValidity = 10;
    private final byte[] token = {1, 2, 3};
    private final IssuedCertificate issued = issuedCertificate();
    private TrustDependencyManager trustDependencyManager;
    private TrustManager trustManager;
    private CertificateOutputWriter<TestId> certificateOutputWriter;
    private CertificateIssuer issuer;
    private Validator validator;
    private CertificateController controller;
    private RequiredCertificate<TestId> requiredCertificate;


    CertificateControllerTest() throws IOException, CertificateException {
    }

    @BeforeEach
    public void before() throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        TrustController trustDependencyManager = mock(TrustController.class);
        this.trustDependencyManager = trustDependencyManager;
        this.trustManager = trustDependencyManager;

        certificateOutputWriter = mock(CertificateOutputWriter.class);

        issuer = mock(CertificateIssuer.class);

        validator = mock(Validator.class);

        controller = new CertificateController(
                new Reconciliation("test", "Kafka", "ns", "n"),
                this.trustDependencyManager,
                issuer,
                validator,
                requestedValidity, false);

        requiredCertificate = new RequiredCertificate<>(subjectIdentity, subject);
    }

    /** Test the transition NEW -> REQUESTED, where the issuer doesn't reply synchronously */
    @Test
    public void testNewToRequested() throws CertificateException {
        when(certificateOutputWriter.loadStatus(subjectIdentity)).thenReturn(CertificateStatus.createNew());

        when(issuer.requestCertificate(eq(subject), eq(requestedValidity))).thenReturn(token);
        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.empty());
        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.noTrustAnchor());

        // Do the reconcile
        CertificateReconcileResult reconcile = controller.reconcile(requiredCertificate, certificateOutputWriter);
        assertEquals(CertificateReconcileResult.WaitingForIssuance.class, reconcile.getClass());

        verify(certificateOutputWriter).loadStatus(subjectIdentity);
        verify(issuer).requestCertificate(eq(subject), eq(requestedValidity));
        verify(issuer).pollForRequestedCertificate(eq(token));
        verify(certificateOutputWriter).storeRequested(eq(subjectIdentity), eq(token));
    }
    @Test
    public void testRequestedToPending() throws CertificateException {
        when(certificateOutputWriter.loadStatus(subjectIdentity)).thenReturn(new CertificateStatus(CertificateState.REQUESTED, token, null));
        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.of(issued));
        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.noTrustAnchor());

        // Do the reconcile again
        CertificateReconcileResult reconcile = controller.reconcile(requiredCertificate, certificateOutputWriter);
        assertEquals(CertificateReconcileResult.WaitingForTrust.class, reconcile.getClass());

        verify(certificateOutputWriter).loadStatus(subjectIdentity);
        verify(issuer, never()).requestCertificate(eq(subject), eq(requestedValidity));
        verify(issuer).pollForRequestedCertificate(eq(token));
        verify(certificateOutputWriter, never()).storeRequested(eq(subjectIdentity), eq(token));
        verify(certificateOutputWriter).storePending(eq(subjectIdentity), eq(issued));
    }

    @Test
    public void testPendingStuckWaitingForTrust() throws CertificateException {
        when(certificateOutputWriter.loadStatus(subjectIdentity)).thenReturn(new CertificateStatus(CertificateState.PENDING, null, issued));
        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.of(issued));
        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.noTrustAnchor());
        when(trustDependencyManager.stateOfRootCert(issued.rootCaFingerprint())).thenReturn(TrustState.UNTRUSTED);
        when(trustDependencyManager.rootCertsInStates(eq(Set.of(TrustState.TRUSTED_NEW, TrustState.TRUSTED_USED)))).thenReturn(Set.of());

        // Do the reconcile again
        CertificateReconcileResult reconcile = controller.reconcile(requiredCertificate, certificateOutputWriter);
        assertEquals(CertificateReconcileResult.WaitingForTrust.class, reconcile.getClass());

        verify(certificateOutputWriter).loadStatus(subjectIdentity);
        verify(issuer, never()).requestCertificate(eq(subject), eq(requestedValidity));
        verify(issuer, never()).pollForRequestedCertificate(eq(token));
        verify(certificateOutputWriter, never()).storeRequested(eq(subjectIdentity), eq(token));
        verify(certificateOutputWriter, never()).storePending(eq(subjectIdentity), eq(issued));
    }

    @Test
    public void testPendingToInUse() throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        when(certificateOutputWriter.loadStatus(subjectIdentity)).thenReturn(new CertificateStatus(CertificateState.PENDING, null, issued));
        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.ok(new TrustAnchor(issued.rootCaCertificate(), null)));
        when(trustDependencyManager.stateOfRootCert(issued.rootCaFingerprint())).thenReturn(TrustState.TRUSTED_NEW);
        when(trustDependencyManager.rootCertsInStates(eq(Set.of(TrustState.TRUSTED_NEW, TrustState.TRUSTED_USED)))).thenReturn(Set.of(issued.rootCaCertificate()));

        // Do the reconcile again
        CertificateReconcileResult reconcile = controller.reconcile(requiredCertificate, certificateOutputWriter);
        assertEquals(CertificateReconcileResult.OkIssued.class, reconcile.getClass());

        verify(certificateOutputWriter).loadStatus(subjectIdentity);
        verify(issuer, never()).requestCertificate(eq(subject), eq(requestedValidity));
        verify(issuer, never()).pollForRequestedCertificate(eq(token));
        verify(certificateOutputWriter, never()).storeRequested(eq(subjectIdentity), eq(token));
        verify(certificateOutputWriter, never()).storePending(eq(subjectIdentity), eq(issued));
        verify(certificateOutputWriter).writeIssued(eq(subjectIdentity), eq(issued));
    }

    @Test
    public void testInUseNoop() throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        when(certificateOutputWriter.loadStatus(subjectIdentity)).thenReturn(new CertificateStatus(CertificateState.IN_USE, null, issued));
        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.ok(new TrustAnchor(issued.rootCaCertificate(), null)));
        when(trustDependencyManager.stateOfRootCert(issued.rootCaFingerprint())).thenReturn(TrustState.TRUSTED_NEW);
        when(trustDependencyManager.rootCertsInStates(eq(Set.of(TrustState.TRUSTED_NEW, TrustState.TRUSTED_USED)))).thenReturn(Set.of(issued.rootCaCertificate()));

        // Do the reconcile again
        CertificateReconcileResult reconcile = controller.reconcile(requiredCertificate, certificateOutputWriter);
        assertEquals(CertificateReconcileResult.OkNoop.class, reconcile.getClass());

        verify(certificateOutputWriter).loadStatus(subjectIdentity);
        verify(issuer, never()).requestCertificate(eq(subject), eq(requestedValidity));
        verify(issuer, never()).pollForRequestedCertificate(eq(token));
        verify(certificateOutputWriter, never()).storeRequested(eq(subjectIdentity), eq(token));
        verify(certificateOutputWriter, never()).storePending(eq(subjectIdentity), eq(issued));
        verify(certificateOutputWriter, never()).writeIssued(eq(subjectIdentity), eq(issued));
    }

}