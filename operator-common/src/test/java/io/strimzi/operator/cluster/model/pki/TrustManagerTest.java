/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.List;

import io.strimzi.api.kafka.model.status.CaCertState;
import io.strimzi.api.kafka.model.status.CaCertStatusBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TrustManagerTest {

    public static final String EXPECTED_FINGERPRINT = "9B7EBFFEAE1B1F86A9991625E561C52D3779E49C";
    private TrustManager trustManager;

    @BeforeEach
    public void before() {
        trustManager = new TrustManager();
    }

    @Test
    void x509Certificate() throws CertificateException, IOException {
        byte[] buffer = certBytes();
        X509Certificate x509Certificate = TrustManager.x509Certificate(buffer);
        assertEquals("CN=cluster-ca, O=Default Company Ltd, L=Default City, C=XX", x509Certificate.getIssuerDN().toString());
        assertNotEquals(-1, x509Certificate.getBasicConstraints(),
                "expected a certificate with CA:true");
        assertEquals(ZonedDateTime.of(2018, 8, 23, 16, 19, 54, 0, ZoneId.of("UTC")).toInstant(), x509Certificate.getNotBefore().toInstant());
        assertEquals(ZonedDateTime.of(2118, 7, 30, 16, 19, 54, 0, ZoneId.of("UTC")).toInstant(), x509Certificate.getNotAfter().toInstant());

    }

    private byte[] certBytes() throws IOException {
        InputStream resourceAsStream = TrustManagerTest.class.getResourceAsStream("/CLUSTER_CERT.pem");
        return resourceAsStream.readAllBytes();
    }

    @Test
    void load() throws IOException, CertificateException {
        assertEquals(new Fingerprint(EXPECTED_FINGERPRINT),
                trustManager.load(new String(certBytes()), CaCertState.UNTRUSTED));
    }

    @Test
    void findCa() throws IOException, CertificateException {
        load();
        assertEquals(new Fingerprint(EXPECTED_FINGERPRINT),
                trustManager.findCa(CaCertState.UNTRUSTED).orElseThrow().fingerprint());
        for (CaCertState s : EnumSet.complementOf(EnumSet.of(CaCertState.UNTRUSTED))) {
            assertFalse(trustManager.findCa(s).isPresent());
        }
    }

    @Test
    void transitionToTrusted() throws IOException, CertificateException {
        load();
        trustManager.transitionToTrusted(new Fingerprint(EXPECTED_FINGERPRINT));
        assertEquals(new Fingerprint(EXPECTED_FINGERPRINT),
                trustManager.findCa(CaCertState.TRUSTED).orElseThrow().fingerprint());
        for (CaCertState s : EnumSet.complementOf(EnumSet.of(CaCertState.TRUSTED))) {
            assertFalse(trustManager.findCa(s).isPresent());
        }
    }

    @Test
    void transitionToPhaseOut() throws IOException, CertificateException {
        load();
        assertThrows(IllegalStateException.class, () -> trustManager.transitionToPhaseOut(new Fingerprint(EXPECTED_FINGERPRINT)));
        trustManager.transitionToTrusted(new Fingerprint(EXPECTED_FINGERPRINT));
        trustManager.transitionToPhaseOut(new Fingerprint(EXPECTED_FINGERPRINT));
        assertEquals(new Fingerprint(EXPECTED_FINGERPRINT),
                trustManager.findCa(CaCertState.PHASE_OUT).orElseThrow().fingerprint());
        for (CaCertState s : EnumSet.complementOf(EnumSet.of(CaCertState.PHASE_OUT))) {
            assertFalse(trustManager.findCa(s).isPresent());
        }
    }

    @Test
    void transitionToZombie() throws IOException, CertificateException {
        load();
        assertThrows(IllegalStateException.class, () -> trustManager.transitionToPhaseOut(new Fingerprint(EXPECTED_FINGERPRINT)));
        trustManager.transitionToTrusted(new Fingerprint(EXPECTED_FINGERPRINT));
        assertThrows(IllegalStateException.class, () -> trustManager.transitionToZombie(new Fingerprint(EXPECTED_FINGERPRINT)));
        trustManager.transitionToPhaseOut(new Fingerprint(EXPECTED_FINGERPRINT));
        trustManager.transitionToZombie(new Fingerprint(EXPECTED_FINGERPRINT));
        assertEquals(new Fingerprint(EXPECTED_FINGERPRINT),
                trustManager.findCa(CaCertState.ZOMBIE).orElseThrow().fingerprint());
        for (CaCertState s : EnumSet.complementOf(EnumSet.of(CaCertState.ZOMBIE))) {
            assertFalse(trustManager.findCa(s).isPresent());
        }
    }

    @Test
    void asStatusList() throws IOException, CertificateException {
        load();
        assertEquals(List.of(new CaCertStatusBuilder()
                .withFingerprint(EXPECTED_FINGERPRINT)
                .withState(CaCertState.UNTRUSTED)
                .withNotBefore("2018-08-23T16:19:54Z")
                .withNotAfter("2118-07-30T16:19:54Z").build()), trustManager.asStatusList());
    }
}