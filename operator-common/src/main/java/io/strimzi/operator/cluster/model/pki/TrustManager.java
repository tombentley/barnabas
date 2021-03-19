/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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

    static interface TrustRootListener {
        void onTransition(TrustRoot trustRoot);
    }

    private final TrustRootListener listener;

    private final Map<Fingerprint, CaCertState> states;
    private final KeyStore trustStore;

    public TrustManager() {
        this(null);
    }
    public TrustManager(TrustRootListener listener) {
        this.states = new HashMap<>();
        try {
            this.trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, null);
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException(e); // PKCS12 should be supported on all modern JVMs.
        }
        this.listener = listener;
    }


    public List<TrustRoot> entries() {
        return states.entrySet().stream().map(e ->
                getWithState(e.getValue(), e.getKey())).collect(Collectors.toList());
    }

    /**
     * Convert the trust roots in this manager which have a state in the given set of {@code states}
     * to a set of {@link TrustAnchor TrustAnchors} for use in PKIX certificate validation.
     * @param states The states for selecting the trust roots to include.
     * @return The trust anchors.
     */
    public Set<TrustAnchor> toTrustAnchors(EnumSet<CaCertState> states) {
        return this.states.entrySet().stream()
                .filter(entry -> states.contains(entry.getValue()))
                .map(entry -> {
                    try {
                        return new TrustAnchor((X509Certificate) this.trustStore.getCertificate(entry.getKey().toString()), null);
                    } catch (KeyStoreException keyStoreException) {
                        throw new RuntimeException(keyStoreException);
                    }
                }).collect(Collectors.toUnmodifiableSet());
    }

    // TODO we need methods to load the state from Kube Secrets
    // We'd load all the certs in the secret
    // If the CR had a status the ca.crt would be TRUSTED and all the others would be PHASE_OUT
    // We'd then update the CR status, then update the Secret contents
    // When the CR has a status we load the certs from the Secret, compute the fingerprint and look up the status


    public Fingerprint put(byte[] cert, CaCertState state) throws CertificateException {
        return doPut(new TrustRoot(PkiUtils.x509Certificate(cert), state));
    }

    public Fingerprint put(X509Certificate cert, CaCertState state) {
        return doPut(new TrustRoot(cert, state));
    }

    private Fingerprint doPut(TrustRoot trustRoot) {
        Fingerprint fingerprint = trustRoot.fingerprint();
        CaCertState caCertState = states.get(fingerprint);
        if (caCertState != null && !trustRoot.state().equals(caCertState)) {
            throw new IllegalStateException("CA certificate " + fingerprint + " is in state" + caCertState + " so cannot be loaded in state " + trustRoot.state());
        }
        try {
            this.trustStore.setCertificateEntry(fingerprint.toString(), trustRoot.x509());
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
        states.put(fingerprint, trustRoot.state());
        fireListener(fingerprint);
        return fingerprint;
    }

    private void fireListener(Fingerprint fingerprint) {
        if (listener != null) {
            listener.onTransition(getWithState(states.get(fingerprint), fingerprint));
        }
    }

    /**
     * @return the state of the Trust root with the given fingerprint, or UNKNOWN if the not certificate with that fingerprint is known.
     */
    public CaCertState state(Fingerprint fingerprint) {
        return states.getOrDefault(fingerprint, CaCertState.UNKNOWN);
    }

    /**
     * Find a certificate in the given state.
     * @param state The state.
     * @return The result.
     */
    public Optional<TrustRoot> findCa(CaCertState state) {
        return states.entrySet().stream()
            .filter(entry -> entry.getValue() == state)
            .map(entry -> {
                Fingerprint key = entry.getKey();
                return getWithState(entry.getValue(), key);
            })
            .filter(c -> c.isValidAt(Instant.now()))
            .max(Comparator.comparing(TrustRoot::notAfter));
    }

    private TrustRoot getWithState(CaCertState state, Fingerprint key) {
        X509Certificate certificate;
        try {
            certificate = (X509Certificate) trustStore.getCertificate(key.toString());
        } catch (KeyStoreException keyStoreException) {
            throw new RuntimeException(keyStoreException);
        }
        return new TrustRoot(certificate, state);
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
        CaCertState caState = states.get(fingerprint);
        if (caState == null) {
            throw new UnknownCertificateException();
        }
        if (!caState.outward.contains(targetState)) {
            throw new IllegalStateException("CA certificate " + fingerprint + " is in state" + caState + " so cannot transition to " + targetState);
        }
        states.put(fingerprint, targetState);
        fireListener(fingerprint);
    }

    public List<CaCertStatus> asStatusList() {
        return states.entrySet().stream()
                .map(entry -> {
                    X509Certificate certificate;
                    try {
                        certificate = (X509Certificate) trustStore.getCertificate(entry.getKey().toString());
                    } catch (KeyStoreException keyStoreException) {
                        throw new RuntimeException(keyStoreException);
                    }
                    return asStatus(new TrustRoot(certificate, entry.getValue()));
                })
                .collect(Collectors.toList());
    }

    private CaCertStatus asStatus(TrustRoot trustRoot) {
        ZoneId utc = ZoneId.of("UTC");
        return new CaCertStatusBuilder().withFingerprint(trustRoot.fingerprint().toString())
                .withState(trustRoot.state())
                .withNotBefore(DATE_TIME_FORMATTER.format(trustRoot.notBefore().atZone(utc)))
                .withNotAfter(DATE_TIME_FORMATTER.format(trustRoot.notAfter().atZone(utc)))
                .build();
    }

}
