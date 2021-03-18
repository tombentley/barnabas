/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import javax.security.auth.x500.X500Principal;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.status.CaCertState;
import io.strimzi.api.kafka.model.status.CaCertStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.operator.cluster.model.pki.PkiUtils.fingerprint;
import static io.strimzi.operator.cluster.model.pki.PkiUtils.x509Certificate;

public abstract class Pki<Cr extends CustomResource<Spec, Status>, Spec, Status> {

    private static final Logger LOGGER = LogManager.getLogger(Pki.class);

    private final CertManager certManager;
    protected final KubernetesClient client;
    protected final String namespace;
    protected final String cluster;
    private final String caSecretName;
    private final String trustSecretName;
    private final CertificateFactory certificateFactory;
    private final CertPathValidator validator;
    private final Clock clock;
    private PendingCertificateManager pendingCertificateManager;

    public Pki(Clock clock, CertManager certManager, KubernetesClient client, String namespace, String cluster,
               String caSecretName, String trustSecretName) {
        this.certManager = certManager;
        this.client = client;
        this.namespace = namespace;
        this.cluster = cluster;
        this.caSecretName = caSecretName;
        this.trustSecretName = trustSecretName;
        try {
            certificateFactory = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new RuntimeException(e); // standard part of Java: should never happen
        }
        try {
            validator = CertPathValidator.getInstance("PKIX");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // standard part of Java: should never happen
        }
        this.clock = clock;
        this.pendingCertificateManager = new DefaultPendingCertificateManager(client, namespace, cluster, caSecretName);
    }

    Resource<Secret> trustSecretRef() {
        return secretRef(trustSecretName);
    }

    Resource<Secret> caSecretRef() {
        return secretRef(caSecretName);
    }


    /**
     * @param cr The custom resource.
     * @return The CertificateAuthority for the given CR.
     */
    protected abstract CertificateAuthority ca(Cr cr);

    protected abstract Stream<RequiredCertificate> requiredCertificates(Cr cr);

    protected abstract List<CaCertStatus> status(Cr cr);

    /**
     * Update the CR status with the state of the trust manager
     */
    protected abstract Cr updateStatus(Cr cr, List<CaCertStatus> statusList);

    /**
     * <p>The first half of the reconciliation, which:</p>
     * <ul>
     *   <li>Discovers required certificate(s)</li>
     *   <li>Issues those certificates, creating some pending certificates, which are stored for later use by {@link #reconcilePending(CustomResource, TrustManager)}</li>
     *   <li>Figures out if any of the pending certificates would need new trust relationships to be established
     *   prior to the pending certificates being used.</li>
     * </ul>
     * @param cr The CR.
     * @param trustManager The trust manager.
     * @return true if there are new trust anchors, requiring action before
     * {@link #reconcilePending(CustomResource, TrustManager)} can be called.
     */
    // TODO load the trust manager from the trust secret state
    public boolean reconcileTrust(Cr cr, TrustManager trustManager) {
        // Look at CR to decide which certs we need
        // TODO we should recognise the .tmp in the secret and reuse them, rather than generating new certs
        List<RequiredCertificate> requiredCertificates = requiredCertificates(cr).collect(Collectors.toList());
        LOGGER.debug("Required certificates: {}", requiredCertificates);
        // Look at secrets to decide which certs we have
        Set<TrustAnchor> possibleTrustAnchors = trustManager.trustStore(EnumSet.of(CaCertState.TRUSTED, CaCertState.PHASE_OUT));
        ValidateResult vr = filter(cr, requiredCertificates, possibleTrustAnchors);
        LOGGER.debug("required trust anchors: {}", vr.requiredTrustAnchors());
        // TODO this isn't quite right. We certs which are currently valid but not in the future needs to transition to PHASE_OUT
        // TODO and the ones which aren't used at all can transition to ZOMBIE
        for (TrustAnchor ta : vr.unneededTrustAnchors(possibleTrustAnchors)) {
            Fingerprint fingerprint = fingerprint(ta.getTrustedCert());
            LOGGER.debug("Transitioning trusted certificate to PHASE_OUT: fingerprint: {}", fingerprint);
            trustManager.transitionToPhaseOut(fingerprint);
        }

        // Issue those certs
        boolean restartRequired = false;
        List<PendingCertificate> pending = new ArrayList<>(vr.certsToIssue().size());
        List<Exception> exceptions = new ArrayList<>(0);
        for (RequiredCertificate requiredCertificate : vr.certsToIssue()) {
            Optional<PendingCertificate> pendingCertificateOpt;
            try {
                pendingCertificateOpt = issueClientCertificate(cr, requiredCertificate, ca(cr).getValidityDays());
            } catch (CertificateException | CertificateIssuanceException e) {
                exceptions.add(e);
                continue;
            }
            if (pendingCertificateOpt.isPresent()) {
                PendingCertificate pendingCertificate = pendingCertificateOpt.get();
                CaCertState state = maybeEstablishTrust(cr, trustManager, requiredCertificate, pendingCertificate.issuedCertificate());
                switch (state) {
                    case UNKNOWN:
                    case UNTRUSTED:
                        pending.add(pendingCertificate);
                        restartRequired = true;
                        break;
                    case TRUSTED:
                        pending.add(pendingCertificate);
                        break;
                    case PHASE_OUT:
                    case ZOMBIE:
                    default:
                        throw new RuntimeException();
                }
            } else {
                // TODO what if the cert isn't issued yet?
            }
        }

        pendingCertificateManager.storePending(pending);

        // TODO Maybe do some restarts to establish trust (what do I do with the issued certs until then? Add them to what secret?
        // wherever it is they'd need to be findable again from the RequiredCertificates stuff

        if (!exceptions.isEmpty()) {
            CertificateIssuanceException ex = new CertificateIssuanceException("Error issuing certificates");
            exceptions.forEach(ex::addSuppressed);
        }
        return restartRequired;
    }

    ValidateResult filter(Cr cr, List<RequiredCertificate> requiredCertificates, Set<TrustAnchor> possibleTrustAnchors) {
        Instant now = clock.instant();
        PKIXParameters pkixParameters = pkixParameters(possibleTrustAnchors);
        ValidateResult vr = new ValidateResult();
        for (RequiredCertificate requiredCertificate : requiredCertificates) {
            // TODO When scaling down, who deletes unneeded certs?
            Secret secret = secretRef(requiredCertificate.secretName()).get();
            if (secret == null || secret.getData().isEmpty()) {
                LOGGER.debug("Secret missing for required certificate: {}", requiredCertificate);
                vr.addCertToIssue(requiredCertificate);
            } else {
                // Check those certs are valid to decide which certs we need to issue/reissue
                String certChain = secret.getData().get(requiredCertificate.certChainEntryName());
                String pendingCertChain = secret.getData().get(requiredCertificate.pendingCertChainEntryName());
                if (certChain == null && pendingCertChain == null) {
                    LOGGER.debug("Secret entry missing for required certificate chain: {}", requiredCertificate);
                    vr.addCertToIssue(requiredCertificate);
                } else if (pendingCertChain != null) {
                    // Validate that the pending certs are still good since they were issued
                    vr.add(validate(cr, now, pkixParameters, requiredCertificate, pendingCertChain, requiredCertificate.pendingCertChainEntryName()));
                } else {
                    // Validate that the existing certs are still good (the usual case)
                    vr.add(validate(cr, now, pkixParameters, requiredCertificate, certChain, requiredCertificate.certChainEntryName()));
                }
            }
        }
        return vr;
    }

    static class ValidateResult {
        private final List<RequiredCertificate> certsToIssue;
        private final Set<TrustAnchor> requiredTrustAnchors;

        public ValidateResult() {
            this.certsToIssue = new ArrayList<>();
            this.requiredTrustAnchors = new HashSet<>();
        }

        public ValidateResult(List<RequiredCertificate> certsToIssue, Set<TrustAnchor> requiredTrustAnchors) {
            this.certsToIssue = certsToIssue;
            this.requiredTrustAnchors = requiredTrustAnchors;
        }

        public void add(ValidateResult result) {
            this.certsToIssue.addAll(result.certsToIssue());
            this.requiredTrustAnchors.addAll(result.requiredTrustAnchors());
        }

        public void addCertToIssue(RequiredCertificate requiredCert) {
            this.certsToIssue.add(requiredCert);
        }

        public List<RequiredCertificate> certsToIssue() {
            return certsToIssue;
        }

        public void addRequiredTrustAnchor(TrustAnchor trustAnchor) {
            this.requiredTrustAnchors.add(trustAnchor);
        }

        public Set<TrustAnchor> requiredTrustAnchors() {
            return requiredTrustAnchors;
        }

        public Set<TrustAnchor> unneededTrustAnchors(Set<TrustAnchor> possibleTrustAnchors) {
            Set<TrustAnchor> unneededTrustAnchors = new HashSet<>(possibleTrustAnchors);
            unneededTrustAnchors.removeAll(requiredTrustAnchors());
            return unneededTrustAnchors;
        }
    }

    ValidateResult validate(Cr cr, Instant now, PKIXParameters pkixParameters, RequiredCertificate requiredCertificate,
                  String certChain, String secretEntry) {
        Objects.requireNonNull(pkixParameters);

        List<RequiredCertificate> certsToIssue = new ArrayList<>();
        Set<TrustAnchor> requiredTrustAnchors = new HashSet<>();
        try {
            // Validate whether there's currently a valid cert path
            List<X509Certificate> chain = PkiUtils.parsePemCertChain(Base64.getDecoder().decode(certChain));
            CertPath certPath = certificateFactory.generateCertPath(chain);
            try {
                requiredTrustAnchors.add(pkixValidate(now, pkixParameters, requiredCertificate, certPath));
            } catch (CertPathValidatorException e) {
                // TODO improve logging of the reason
                LOGGER.info("No valid cert path: secret entry: {}, params: {}, cert: {}, reason: {}",
                        secretEntry, pkixParameters, requiredCertificate, describeCertPathFailure(e));
                certsToIssue.add(requiredCertificate);
            }

            // Validate whether there will still be a valid cert path in $renewalPeriod days time
            Instant date = now.plus(ca(cr).getRenewalDays(), ChronoUnit.DAYS);
            try {
                requiredTrustAnchors.add(pkixValidate(date, pkixParameters, requiredCertificate, certPath));
            } catch (CertPathValidatorException e) {
                // TODO improve logging of the reason
                LOGGER.info("No valid cert path: secret entry: {}, params: {}, cert: {}, reason: {}",
                        secretEntry, pkixParameters, requiredCertificate, describeCertPathFailure(e));
                CertPathValidatorException.Reason reason = e.getReason();
                if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                    certsToIssue.add(requiredCertificate);
                    if (e.getIndex() == 0) {
                        // if the leaf cert itself is the problem then we just need to reissue
                    } else {
                        // TODO it's a root or intermediary: transition to phase out as well
                    }
                } else {
                    // TODO it's some other reason: transition to phase out as well
                }
            }
        } catch (CertificateException e) {
            LOGGER.debug("Secret entry cannot be read as a certificate: {}", requiredCertificate);
            certsToIssue.add(requiredCertificate);
        }
        return new ValidateResult(certsToIssue, requiredTrustAnchors);
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
            TrustAnchor trustAnchor = ((PKIXCertPathValidatorResult) validator.validate(certPath, pkixParameters)).getTrustAnchor();
            LOGGER.debug("Valid cert path exists at {}: cert: {}, trustAnchor: {}", date, requiredCertificate, trustAnchor);
            return trustAnchor;
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e); // should never happen
        }
    }

    PKIXParameters pkixParameters(Set<TrustAnchor> possibleTrustAnchors) {
        PKIXParameters pkixParameters;
        if (possibleTrustAnchors.isEmpty()) {
            pkixParameters = null;
        } else {
            try {
                pkixParameters = new PKIXParameters(possibleTrustAnchors);
            } catch (InvalidAlgorithmParameterException e) {
                throw new RuntimeException(e);
            }
            pkixParameters.setRevocationEnabled(false);
        }
        return pkixParameters;
    }


    /**
     * @param cr The custom resource
     * @return a TrustManager with state loaded from the Secret for the given cluster.
     * @throws CertificateException Cert problems
     */
    public TrustManager loadTrust(Cr cr) throws CertificateException {
        TrustManager trustManager;

        List<CaCertStatus> clusterCa = status(cr);
        Secret secret = trustSecretRef().get();
        Map<String, String> data = secret == null ? null : secret.getData();
        if (clusterCa == null || clusterCa.isEmpty()) {
            if (data == null) {
                // bootstrap: do nothing: a new CA will be created when we get generating client certs
                trustManager = new TrustManager();
            } else {
                trustManager = convertCertsSecret(cr, data);
            }
        } else {
            if (data == null) {
                // heal: do nothing: a new CA will be created when we get generating client certs
                trustManager = new TrustManager();
            } else {
                // normal load case
                trustManager = loadFromSecret(data);
            }
        }
        return trustManager;
    }

//    void loadFromBoth(TrustManager trustManager, List<CaCertStatus> clusterCa, Map<String, String> data) throws CertificateException {
//        // load certs from the Secret values
//        Map<Fingerprint, String> collect = data.values().stream().collect(Collectors.toMap(s -> {
//            try {
//                return TrustManager.fingerprint(x509Certificate(Base64.getDecoder().decode(s)));
//            } catch (CertificateException e) {
//                throw new RuntimeException(e);
//            }
//        }, s -> s));
//        // get the state from the CR status
//        for (CaCertStatus caCertStatus : clusterCa) {
//            String s = collect.get(new Fingerprint(caCertStatus.getFingerprint()));
//            trustManager.load(s, caCertStatus.getState());
//        }
//    }

    TrustManager loadFromSecret(Map<String, String> data) throws CertificateException {
        TrustManager trustManager = new TrustManager();
        Pattern pattern = Pattern.compile("(?<fingerprint>[0-9A-Z]{40})\\.(?<state>ZOMBIE|PHASE_OUT|TRUSTED|UNTRUSTED)\\.crt", Pattern.CASE_INSENSITIVE);
        // load certs from the Secret values
        for (Map.Entry<String, String> e : data.entrySet()) {
            Matcher matcher = pattern.matcher(e.getKey());
            if (matcher.matches()) {
                CaCertState state = CaCertState.valueOf(matcher.group("state"));
                Fingerprint loaded = trustManager.put(new String(Base64.getDecoder().decode(e.getValue())), state);
                Fingerprint fingerprint = new Fingerprint(matcher.group("fingerprint"));
                if (!loaded.equals(fingerprint)) {
                    throw new IllegalStateException("Certificate in entry " + e.getKey() + " actually has fingerprint " + loaded);
                }
            }
        }
        return trustManager;
    }

    /**
     * Converts from certs Secret with ca.crt etc to one with fingerprint.crt
     * @return
     */
    private TrustManager convertCertsSecret(Cr cr, Map<String, String> data) throws CertificateException {
        TrustManager trustManager = new TrustManager();
        // convert
        SecretBuilder sb = new SecretBuilder();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            Fingerprint fingerprint;
            if (entry.getKey().equals("ca.crt")) {
                fingerprint = trustManager.put(entry.getValue(), CaCertState.TRUSTED);
            } else if (entry.getKey().endsWith(".crt")) {
                fingerprint = trustManager.put(entry.getValue(), CaCertState.PHASE_OUT);
            } else {
                continue;
            }
            sb.addToData(fingerprint.toString(), entry.getValue());
        }
        client.secrets().createOrReplace(sb.build());

        // TODO check that we don't need to need to update the p12? Or maybe we should just regenerate it.

        // update the status
        updateStatus(cr, trustManager.asStatusList());
        return trustManager;
    }

    /**
     * Issues a client (i.e. leaf) certificate and key, storing the key and cert chain
     * in the relevant secret as .tmp files for consumption by the 2nd half.
     * @param cr The custom resource.
     * @param requiredCertificate The cert to issue.
     * @param requestedValidity The requested validity period.
     * @return true iff a an untrusted root certificate was used, so that relying parties need to have their trust
     * stores updated before being exposed to the newly issued certificate.
     * @throws CertificateIssuanceException the requested certificate could not be issued
     * @throws CertificateException problem while validating the issued certificate
     */
    public Optional<PendingCertificate> issueClientCertificate(Cr cr,
                                          RequiredCertificate requiredCertificate, int requestedValidity)
            throws CertificateException, CertificateIssuanceException {
        CertificateIssuer issuer = getIssuer(cr);

        // Issue a certificate
        Subject subject = requiredCertificate.subject();
        // TODO does ${subject}.requested already exist? If not issue it?
        byte[] token = issuer.requestCertificate(subject, requestedValidity);
        // TODO and persist the token in the broker's secret as ${subject}.requested
        // If ${subject}.requested does already exist then poll?
        Optional<IssuedCertificate> issuedCertificate = issuer.pollForRequestedCertificate(token);
        return issuedCertificate.map(issued -> {
            try {
                validateIssuedCertificate(subject, issuedCertificate.get());
            } catch (CertificateParsingException e) {
                throw new RuntimeException(e);
            }
            return new PendingCertificate(requiredCertificate, issued);
        });



        // TODO We make the certificate TRUSTED in the trust manager and update the CR status
        // TODO We can now update the client Secret and roll to trigger stuff to use it.

        // TODO is this actually two methods? Or a single method which results in 3 successive reconciliations?
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

    CaCertState maybeEstablishTrust(Cr cr, TrustManager trustManager, RequiredCertificate requiredCertificate,
                             IssuedCertificate issuedCert) {
        X509Certificate rootCaCertificate = issuedCert.rootCaCertificate();
        CaCertState state = trustManager.state(fingerprint(rootCaCertificate));
        switch (state) {
            case UNKNOWN:
                // It's a new CA cert, or a new CA, or a new cluster
                Secret secret = trustSecretRef().get();
                if (secret == null) {
                    // TODO
                } else {
                    // TODO
                    secret.getData().put(requiredCertificate.subject() + "something", "some state");
                    trustSecretRef().createOrReplace(secret);
                }

                // We add the certificate to trust to the trust store in the init state.

                trustManager.put(rootCaCertificate, CaCertState.UNTRUSTED);
                // TODO update CR status and the Secret
                storeTrustState(trustManager, cr);
            case UNTRUSTED:
                // Already untrusted, we can move on to updating the client Secrets
                break;
            case TRUSTED:
                // Already trusted, we can move on to updating the client Secrets
                break;
            case ZOMBIE:
            case PHASE_OUT:
                // Should not happen that a CA issues a certificate signed by a root CA cert in PHASE_OUT or ZOMBIE
                throw new IllegalStateException("CA issued a certificate using a root CA cert in " + state);
        }
        return state;
    }


    private void storePending(String secretName, List<PendingCertificate> pendingCertificates) {

    }

    private Resource<Secret> secretRef(String secretName) {
        return client.secrets().inNamespace(namespace).withName(secretName);
    }


    /** Instantiates a CertificateIssuer according to the spec in the given CR */
    CertificateIssuer getIssuer(Cr cr) throws CertificateException {
        // get the CA issuance options from the CR spec
        // Instantiate a corresponding impl
        CertificateIssuer issuer = null;
        if (ca(cr).isGenerateCertificateAuthority()) {
            Secret caSecret = caSecretRef().get();
            if (caSecret == null || caSecret.getData() == null || caSecret.getData().isEmpty()) {
                LOGGER.debug("Constructing new StrimziCertificateIssuer due to missing or empty Secret data: secret: {}", caSecretName);
                // Generate a new one
                //   The Strimzi impl may, by side-effect, create a key pair if one doesn't exit
                int validityDays = ca(cr).getValidityDays();
                StrimziCertificateIssuer strimziIssuer = StrimziCertificateIssuer.createRootIssuer(
                        clock,
                        certManager,
                        new Subject.Builder()
                                .withCommonName("cluster CA")
                                .withOrganizationName("Strimzi")
                            .build(),
                        validityDays);
                // For the StrimziCert issuer we need to store the key in the secret
                // (the cert will be stored later)
                storeCaKey(strimziIssuer);
                issuer = strimziIssuer;
            } else {
                // TODO which certificate should I use??? obviously the trusted one, if there is one
                Secret trustSecret = trustSecretRef().get();
                issuer = findIssuer(trustSecret, caSecret, CaCertState.TRUSTED);
                if (issuer == null) {
                    issuer = findIssuer(trustSecret, caSecret, CaCertState.UNTRUSTED);
                }
                if (issuer == null) {
                    throw new RuntimeException("No suitable issuer found");
                }
            }
        } else {
            // TODO
            issuer = null;
        }
        return issuer;
    }

    void storeCaKey(StrimziCertificateIssuer strimziIssuer) {
        Fingerprint fingerprint = fingerprint(strimziIssuer.issuingCert());
        Secret caSecret;
        LOGGER.debug("Storing CA Secret: {}", caSecretName);
        caSecret = new SecretBuilder().withNewMetadata()
                    .withName(caSecretName)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData(fingerprint + ".key", Base64.getEncoder().encodeToString(strimziIssuer.issuingKey()))
                .build();
        caSecretRef().createOrReplace(caSecret);
    }

    private CertificateIssuer findIssuer(Secret trustSecret, Secret caSecret, CaCertState state) throws CertificateException {
        for (Map.Entry<String, String> entry : trustSecret.getData().entrySet()) {
            // TODO we're just picking the first item which ends with `.${state}.crt` at random
            // Which should be pick if there's >1? Probably the one with the latest notAfter.
            String suffix = "." + state + ".crt";
            if (entry.getKey().endsWith(suffix)) {
                LOGGER.debug("Creating new StrimziCertificateIssuer using existing CA secret: secret: {}", caSecretName);
                // Use the existing key
                byte[] caKey = Base64.getDecoder().decode(caSecret.getData().get(entry.getKey().replace(suffix, "") + ".key"));
                byte[] caCert = Base64.getDecoder().decode(entry.getValue());
                return new StrimziCertificateIssuer(clock, certManager, caKey, x509Certificate(caCert), List.of(x509Certificate(caCert)));
            }
        }
        return null;
    }

    /**
     * Persist the state of the trust manager to the cert secret
     * @param trustManager
     */
    void storeTrustState(TrustManager trustManager, Cr cr) {
        SecretBuilder secretBuilder = new SecretBuilder()
                .editOrNewMetadata()
                    .withName(trustSecretName)
                    .withNamespace(namespace)
                .endMetadata();

        for (TrustManager.CaState state : trustManager.entries()) {
            try {
                secretBuilder.addToData(state.fingerprint() + "." + state.state() + ".crt",
                        StandardCharsets.ISO_8859_1.decode(Base64.getEncoder().encode(PkiUtils.asPemBuffer(state.x509()))).toString());
            } catch (CertificateEncodingException e) {
                throw new RuntimeException("Could not encode certificate: " + state.fingerprint(), e);
            }
        }

        Secret build = secretBuilder.build();
        trustSecretRef().createOrReplace(build);
        updateStatus(cr, trustManager.asStatusList());
    }

    /**
     * The 2nd half of the reconciliation, runs after {@link #reconcileTrust(CustomResource, TrustManager)}.
     *
     * Preconditions:
     *
     * 1. That {@link #reconcileTrust(CustomResource, TrustManager)} has already run.
     * 2. That any pod restarts have been done and therefore that all the pending certificates will now be trusted.
     * @param cr The CR
     * @param trustManager The trust manager
     * @throws RestoreException The state could not be restored
     */
    public void reconcilePending(Cr cr, TrustManager trustManager) {
        // TODO somehow we need to pass the pendingcerts to this method.
        // Either we persist that state somewhere or, because we assume reconcileTrust always runs first
        // we just pass it out reconcile1 and into reconcile2

        // If we got this far we can transition the trust manager to TRUSTED
        trustManager.entries().stream()
                .filter(p ->
                        p.state() == CaCertState.UNKNOWN || p.state() == CaCertState.UNTRUSTED)
                .forEach(p -> trustManager.transitionToTrusted(p.fingerprint()));
        storeTrustState(trustManager, cr);

        pendingCertificateManager.restore();

        // Let the normal reconciliation restart brokers with new certs

        // TODO what about the transition to PHASE_OUT?
        // TODO what about the transition to ZOMBIE?
    }

}
