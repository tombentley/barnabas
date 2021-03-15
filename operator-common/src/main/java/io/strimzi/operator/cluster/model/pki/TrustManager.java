/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.strimzi.api.kafka.model.status.CaCertState;
import io.strimzi.api.kafka.model.status.CaCertStatus;
import io.strimzi.api.kafka.model.status.CaCertStatusBuilder;


/**
 * Manages CA certificates and their state.
 * Does <strong>not</strong> manage the corresponding keys.
 * Note that these certs are the trust anchors, they may not be the CA certs used to sign issued certificates
 * (e.g. if intermediate CAs are being used to issued client certs).
 */
public class TrustManager {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendOffsetId().toFormatter();

    public Set<TrustAnchor> trustStore(EnumSet<CaCertState> trusted) {
        return certs.entrySet().stream()
                .filter(entry -> trusted.contains(entry.getValue()))
                .map(entry -> {
                    try {
                        return new TrustAnchor((X509Certificate) this.trustStore.getCertificate(entry.getKey().toString()), null);
                    } catch (KeyStoreException keyStoreException) {
                        throw new RuntimeException(keyStoreException);
                    }
                }).collect(Collectors.toSet());
    }

    static class CaState {
        private final CaCertState state;
        private final X509Certificate x509;

        public CaState(byte[] cert, CaCertState state) throws CertificateException {
            this.x509 = x509Certificate(cert);
            this.state = state;
        }

        public CaState(X509Certificate x509, CaCertState state) {
            this.state = state;
            this.x509 = x509;
        }

        public Fingerprint fingerprint() {
            return TrustManager.fingerprint(x509());
        }

        private X509Certificate x509() {
            return x509;
        }

        String asPem() throws CertificateEncodingException {
            return Pem.asPem(x509());
        }

        public CaCertState state() {
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

        private CaCertStatus asStatus() {
            ZoneId utc = ZoneId.of("UTC");
            return new CaCertStatusBuilder().withFingerprint(fingerprint().toString())
                    .withState(state())
                    .withNotBefore(DATE_TIME_FORMATTER.format(notBefore().atZone(utc)))
                    .withNotAfter(DATE_TIME_FORMATTER.format(notAfter().atZone(utc)))
                    .build();
        }
    }

    public static Fingerprint fingerprint(X509Certificate x509Certificate) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA1");
            return new Fingerprint(sha1.digest(x509Certificate.getEncoded()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // SHA1 should always be supported
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e); // Should never happen
        }
    }

    public static X509Certificate x509Certificate(byte[] cert) throws CertificateException {
        CertificateFactory certificateFactory;
        try {
            certificateFactory = CertificateFactory.getInstance("X509");
        } catch (CertificateException e) {
            throw new RuntimeException(e); // X509 should always be supported
        }
        X509Certificate x509 = (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(cert));
        return x509;
    }

    private final Map<Fingerprint, CaCertState> certs;
    private final KeyStore trustStore;
    private PKIXParameters getPkixParameters() throws InvalidAlgorithmParameterException {
        try {
            return new PKIXParameters(trustStore);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public List<CaState> entries() {
        return certs.entrySet().stream().map(e -> {
            return getWithState(e.getValue(), e.getKey());
        }).collect(Collectors.toList());
    }

    public TrustManager() {
        this.certs = new HashMap<>();
        try {
            this.trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, null);
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException(e); // PKCS12 should be supported on all modern JVMs.
        }
    }

    // TODO we need methods to load the state from Kube Secrets
    // We'd load all the certs in the secret
    // If the CR had a status the ca.crt would be TRUSTED and all the others would be PHASE_OUT
    // We'd then update the CR status, then update the Secret contents
    // When the CR has a status we load the certs from the Secret, compute the fingerprint and look up the status
    public Fingerprint load(String cert, CaCertState state) throws CertificateException {
        return doLoad(new CaState(cert.getBytes(), state));
    }

    public Fingerprint load(X509Certificate cert, CaCertState state) throws CertificateException {
        return doLoad(new CaState(cert, state));
    }

    private Fingerprint doLoad(CaState caState) {
        Fingerprint fingerprint = caState.fingerprint();
        try {
            this.trustStore.setCertificateEntry(fingerprint.toString(), caState.x509());
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
        if (!certs.containsKey(fingerprint)) {
            certs.put(fingerprint, caState.state());
        } else {
            throw new IllegalStateException("CA certificate " + fingerprint + " is in state" + certs.get(fingerprint) + " so cannot be loaded in state " + caState.state);
        }
        return fingerprint;
    }

    public CaCertState state(Fingerprint fingerprint) {
        return certs.getOrDefault(fingerprint, CaCertState.UNKNOWN);
    }

    /**
     * Find the CA certificate
     * @param state
     * @return
     */
    public Optional<CaState> findCa(CaCertState state) {
        return certs.entrySet().stream()
            .filter(entry -> entry.getValue() == state)
            .map(entry -> {
                Fingerprint key = entry.getKey();
                return getWithState(entry.getValue(), key);
            })
            .filter(c -> c.isValidAt(Instant.now()))
            .max(Comparator.comparing(CaState::notAfter));
    }

    private CaState getWithState(CaCertState state, Fingerprint key) {
        X509Certificate certificate;
        try {
            certificate = (X509Certificate) trustStore.getCertificate(key.toString());
        } catch (KeyStoreException keyStoreException) {
            throw new RuntimeException(keyStoreException);
        }
        return new CaState(certificate, state);
    }

    /**
     * Make the given CA cert to {@link CaCertState#TRUSTED}.
     * @param fingerprint The fingerprint of the certificate to transition
     */
    public void transitionToTrusted(Fingerprint fingerprint) {
        transition(fingerprint, CaCertState.TRUSTED);
    }

    /**
     * Make the given CA cert to {@link CaCertState#PHASE_OUT}.
     * @param fingerprint The fingerprint of the certificate to transition
     */
    public void transitionToPhaseOut(Fingerprint fingerprint) {
        transition(fingerprint, CaCertState.PHASE_OUT);
    }

    /**
     * Make the given CA cert to {@link CaCertState#ZOMBIE}.
     * @param fingerprint The fingerprint of the certificate to transition
     */
    public void transitionToZombie(Fingerprint fingerprint) {
        transition(fingerprint, CaCertState.ZOMBIE);
    }

    private void transition(Fingerprint fingerprint, CaCertState targetState) {
        CaCertState caState = certs.get(fingerprint);
        if (caState == null) {
            throw new UnknownCertificateException();
        }
        if (!caState.outward.contains(targetState)) {
            throw new IllegalStateException("CA certificate " + fingerprint + " is in state" + caState + " so cannot transition to " + targetState);
        }
        certs.put(fingerprint, targetState);
    }

    public List<CaCertStatus> asStatusList() {
        return certs.entrySet().stream()
                .map(entry -> {
                    X509Certificate certificate;
                    try {
                        certificate = (X509Certificate) trustStore.getCertificate(entry.getKey().toString());
                    } catch (KeyStoreException keyStoreException) {
                        throw new RuntimeException(keyStoreException);
                    }
                    return new CaState(certificate, entry.getValue()).asStatus();
                })
                .collect(Collectors.toList());
    }

}
