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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.strimzi.certs.Subject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CertificateControllerTest extends OpensslCaTestHarness {

    static class TestId extends SubjectIdentity {

        private final String name;

        public TestId(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestId textId = (TestId) o;
            return Objects.equals(name, textId.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return "TestId(" +
                    "name='" + name + '\'' +
                    ')';
        }
    }

    private final TestId subjectIdentity = new TestId("foo");
    private final Subject subject = new Subject.Builder().withCommonName("My Entity Cert").build();
    private final int requestedValidity = 10;
    private final byte[] token = {1, 2, 3};
    private final IssuedCertificate issued = issuedCertificate();
    private TrustPrivateState trustPrivateState;
    private LinkedHashMap<Object, Object> storedStatuses;
    private CertificateStatusStore certificateStatusStore;
    private ArrayList<IssuedCertificate> observedIssuedCertificates;
    private CertificateOutputWriter certificateOutputWriter;
    private CertificateIssuer issuer;
    private Validator validator;
    private CertificateController controller;
    private Map<Fingerprint, TrustRoot> trustPrivateStateMap;

    CertificateControllerTest() throws IOException, CertificateException {
    }

    @BeforeEach
    public void before() throws CertificateException, InvalidKeySpecException, KeyStoreException, IOException {
        trustPrivateStateMap = new HashMap<>();
        trustPrivateState = mock(TrustPrivateState.class);
        doAnswer(i -> {
            trustPrivateStateMap.putAll(i.getArgument(0));
            return null;
        }).when(trustPrivateState).transition(any());
        when(trustPrivateState.load()).thenAnswer(i -> new HashMap<>(trustPrivateStateMap));

        storedStatuses = new LinkedHashMap<>();
        certificateStatusStore = mock(CertificateStatusStore.class);
        when(certificateStatusStore.loadStatus(subjectIdentity)).thenAnswer(i -> storedStatuses.get(i.getArgument(0)));
        Mockito.doAnswer(i -> {
            CertificateStatus status = i.getArgument(0);
            storedStatuses.put(status.subjectIdentity(), status);
            return null;
        }).when(certificateStatusStore).storeStatus(any());

        observedIssuedCertificates = new ArrayList<>();
        certificateOutputWriter = mock(CertificateOutputWriter.class);
        doAnswer(i -> {
            assertEquals(subjectIdentity, i.getArgument(0));
            observedIssuedCertificates.add(i.getArgument(1));
            return null;
        }).when(certificateOutputWriter).writeIssued(any(), any(), any());

        issuer = mock(CertificateIssuer.class);

        validator = mock(Validator.class);

        controller = new CertificateController(
                trustPrivateState,
                certificateStatusStore,
                List.of(certificateOutputWriter),
                issuer,
                validator,
                requestedValidity, null);
    }

    /** Test the transition NEW -> REQUESTED, where the issuer doesn't reply synchronously */
    @Test
    public void testNewToRequested() throws CertificateException {
        when(trustPrivateState.load()).thenReturn(Map.of());

        storedStatuses.put(subjectIdentity, newStatus());

        when(issuer.requestCertificate(eq(subject), eq(requestedValidity))).thenReturn(token);
        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.empty());

        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(Validator.Result.noTrustAnchor());

        // Do the reconcile
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        final CertificateStatus requested = requestedStatus();
        assertEquals(List.of(requested), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(), observedIssuedCertificates);
        assertEquals(Map.of(), trustPrivateStateMap);

        // Nothing should change if we reconcile again, but in particular we should not request another new certificate
        when(issuer.requestCertificate(eq(subject), eq(requestedValidity)))
                .thenThrow(new AssertionError("Should not try to issue another cert"));
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(requested), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(), observedIssuedCertificates);
        assertEquals(Map.of(), trustPrivateStateMap);
    }

    private CertificateStatus newStatus() {
        return CertificateStatus.createNew(subjectIdentity);
    }

    private CertificateStatus requestedStatus() {
        return new CertificateStatus(
                CertificateState.REQUESTED,
                subjectIdentity,
                token, null, null);
    }

    private CertificateStatus pendingStatus(IssuedCertificate issuedCertificate) {
        return new CertificateStatus(
                CertificateState.PENDING,
                subjectIdentity,
                null, issuedCertificate, null);
    }

    private CertificateStatus inUseStatus(IssuedCertificate issuedCertificate) {
        return new CertificateStatus(
                CertificateState.IN_USE,
                subjectIdentity,
                null, issuedCertificate, null);
    }

    private CertificateStatus notNeededStatus(IssuedCertificate issuedCertificate) {
        return new CertificateStatus(
                CertificateState.NOT_NEEDED,
                subjectIdentity,
                null, issuedCertificate, null);
    }

    /** Test the transition REQUESTED -> PENDING, where the issuer has replied */
    @Test
    public void testRequestedToPendingWithNothingIssued() throws CertificateException {
        doRequestedToPending(List.of());
    }

    @Test
    public void testRequestedToPendingWithExistingIssued() throws CertificateException {
        observedIssuedCertificates.add(issued);
        doRequestedToPending(List.of(issued));
    }

    private void doRequestedToPending(List<IssuedCertificate> expectedIssuedCertificates) throws CertificateException {
        when(trustPrivateState.load()).thenReturn(Map.of());

        storedStatuses.put(subjectIdentity, requestedStatus());

        when(issuer.requestCertificate(eq(subject), eq(requestedValidity)))
                .thenThrow(new AssertionError("Should not try to issue again"));

        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.of(issued));

        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(
                Validator.Result.noTrustAnchor());

        // Do the reconcile
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(pendingStatus(issued)), List.copyOf(storedStatuses.values()));
        assertEquals(expectedIssuedCertificates, observedIssuedCertificates);
        TrustRoot v1 = new TrustRoot(issued.rootCaCertificate(), TrustState.UNTRUSTED, null);
        assertEquals(Map.of(v1.fingerprint(), v1), trustPrivateStateMap);

        // And do it again, should be idempotent
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(pendingStatus(issued)), List.copyOf(storedStatuses.values()));
        assertEquals(expectedIssuedCertificates, observedIssuedCertificates);
        assertEquals(Map.of(v1.fingerprint(), v1), trustPrivateStateMap);
    }

    /** Test the transition PENDING -> IN_USE, where the root is now trusted */
    @Test
    public void testPendingToInUseWhenRootIsTrusted() throws CertificateException {
        trustPrivateStateMap.put(PkiUtils.fingerprint(issued.rootCaCertificate()),
                TrustRoot.untrusted(issued.rootCaCertificate()).toTrusted("123"));

        doPendingToInUse(TrustState.TRUSTED);
    }

    /** Test the transition PENDING -> IN_USE, where the root is in phase out, but there's no need to renew */
    @Test
    public void testPendingToInUseWhenRootIsPhaseOut() throws CertificateException {
        trustPrivateStateMap.put(PkiUtils.fingerprint(issued.rootCaCertificate()),
                TrustRoot.untrusted(issued.rootCaCertificate()).toTrusted("123").toPhaseOut());

        doPendingToInUse(TrustState.PHASE_OUT);
    }

    private void doPendingToInUse(TrustState expectedState) throws CertificateException {
        storedStatuses.put(subjectIdentity, pendingStatus(issued));

        when(issuer.requestCertificate(eq(subject), eq(requestedValidity)))
                .thenThrow(new AssertionError("Should not try to issue"));

        when(issuer.pollForRequestedCertificate(eq(token)))
                .thenThrow(new AssertionError("Should not try to poll"));

        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(
                Validator.Result.ok(new TrustAnchor(issued.rootCaCertificate(), null)));

        // Do the reconcile
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(inUseStatus(issued)), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(issued), observedIssuedCertificates);
        TrustRoot v1 = new TrustRoot(issued.rootCaCertificate(), expectedState, "123");
        assertEquals(Map.of(v1.fingerprint(), v1), trustPrivateStateMap);

        // And do it again, should be idempotent
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(inUseStatus(issued)), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(issued), observedIssuedCertificates);
        assertEquals(Map.of(v1.fingerprint(), v1), trustPrivateStateMap);
    }

    /** Test the transition IN_USE -> NOT_NEEDED */
    @Test
    public void testInUseToNotNeeded() throws CertificateException {
        TrustRoot value = TrustRoot.untrusted(issued.rootCaCertificate()).toTrusted("123");
        trustPrivateStateMap.put(PkiUtils.fingerprint(issued.rootCaCertificate()),
                value);

        storedStatuses.put(subjectIdentity, inUseStatus(issued));

        // We're going to have to issue a new cert
        when(issuer.requestCertificate(eq(subject), eq(requestedValidity))).thenReturn(token);
        when(issuer.pollForRequestedCertificate(eq(token))).thenReturn(Optional.empty());

        observedIssuedCertificates.add(issued);

        when(validator.validate(any(IssuedCertificate.class), any(Set.class))).thenReturn(
                // TODO another test when it's leaf or intermediate which is expiring.
                Validator.Result.willExpire(issued.rootCaCertificate(), new TrustAnchor(issued.rootCaCertificate(), null)));

        // Do the reconcile
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(requestedStatus()), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(issued), observedIssuedCertificates);
        // TODO really in the PHASE_OUT state? Or should this be zombie????
        assertEquals(Map.of(value.fingerprint(), value.toPhaseOut()), trustPrivateStateMap);

        // And do it again, should be idempotent
        controller.reconcile(new RequiredCertificate(subjectIdentity, subject));

        assertEquals(List.of(requestedStatus()), List.copyOf(storedStatuses.values()));
        assertEquals(List.of(issued), observedIssuedCertificates);
        // TODO really in the PHASE_OUT state? Or should this be zombie????
        assertEquals(Map.of(value.fingerprint(), value.toPhaseOut()), trustPrivateStateMap);
    }

    // TODO assertions on the result of reconcile, if we really need one???
    // TODO the transition to NOT_NEEDED
    // TODO test with different validator results
    // Out of interest... TODO when the issuer throws
}