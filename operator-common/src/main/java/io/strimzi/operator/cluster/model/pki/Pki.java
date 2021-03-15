/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.operator.cluster.model.pki.TrustManager.fingerprint;
import static io.strimzi.operator.cluster.model.pki.TrustManager.x509Certificate;

public abstract class Pki<Cr extends CustomResource<Spec, Status>, Spec, Status> {

    private static final Logger logger = LogManager.getLogger(Pki.class);

    private final CertManager certManager;
    protected final KubernetesClient client;
    protected final String namespace;
    protected final String cluster;
    private final String caSecretName;
    private final String trustSecretName;

    public Pki(CertManager certManager, KubernetesClient client, String namespace, String cluster, String caSecretName, String trustSecretName) {
        this.certManager = certManager;
        this.client = client;
        this.namespace = namespace;
        this.cluster = cluster;
        this.caSecretName = caSecretName;
        this.trustSecretName = trustSecretName;
    }

    Resource<Secret> trustSecretRef() {
        return client.secrets().inNamespace(namespace).withName(trustSecretName);
    }

    Resource<Secret> caSecretRef() {
        return client.secrets().inNamespace(namespace).withName(caSecretName);
    }

    // TODO load the trust manager from the trust secret state
    public boolean reconcile(Cr cr, TrustManager trustManager) throws CertificateException {
        // Look at CR to decide which certs we need
        // TODO we should recognise the .tmp in the secret and reuse them, rather than generating new certs
        long t0 = System.nanoTime();
        List<RequiredCertificate> requiredCertificates = requiredCertificates(cr).collect(Collectors.toList());
        logger.debug("Required certificates: {}", requiredCertificates);
        // Look at secrets to decide which certs we have
        List<RequiredCertificate> missingCertificates = requiredCertificates.stream().filter(requiredCertificate -> {
            // TODO When scaling down, who deletes unneeded certs?
            Secret secret = client.secrets().inNamespace(namespace).withName(requiredCertificate.getSecretName()).get();
            if (secret == null || secret.getData().isEmpty()) {
                logger.debug("Secret missing for required certificate: {}", requiredCertificate);
                return true;
            } else {
                // Check those certs are valid to decide which certs we need to issue/reissue
                String certChain = secret.getData().get(requiredCertificate.getCertChainEntryName());
                if (certChain == null) {
                    logger.debug("Secret entry missing for required certificate chain: {}", requiredCertificate);
                    return true;
                } else {
                    // do cert path validation (and logging)
                    try {
                        List<X509Certificate> chain = Pem.parseCertChain(new String(Base64.getDecoder().decode(certChain)));
                        int renewalDays = ca(cr).getRenewalDays();
                        return !validateCertificate(chain, trustManager.trustStore(EnumSet.of(CaCertState.TRUSTED, CaCertState.PHASE_OUT)),
                                renewalDays);
                    } catch (CertificateException e) {
                        logger.debug("Secret entry cannot be read as a certificate: {}", requiredCertificate);
                        return true;
                    }

                }
            }
        }).collect(Collectors.toList());
        logger.debug("missing certificates: {}", requiredCertificates);

        // Issue those certs
        boolean restartRequired = false;
        for (RequiredCertificate requiredCertificate : missingCertificates) {
            restartRequired |= issueClientCertificate(cr, trustManager, requiredCertificate, ca(cr).getValidityDays());
        }
        // TODO Maybe do some restarts to establish trust (what do I do with the issued certs until then? Add them to what secret?
        // wherever it is they'd need to be findable again from the RequiredCertificates stuff
        return restartRequired;
    }

    /**
     * @param cr The custom resource.
     * @return The CertificateAuthority for the given CR.
     */
    protected abstract CertificateAuthority ca(Cr cr);

    protected abstract Stream<RequiredCertificate> requiredCertificates(Cr cr);

    protected abstract List<CaCertStatus> status(Cr cr);

    /**
     * The 2nd half of the reconciliation.
     *
     * Preconditions:
     *
     * 1. That the 1st half has already run.
     * 2. That any pod restarts have been done.
     * @param cr
     * @param trustManager
     * @throws CertificateException
     */
    public void reconcile2(Cr cr, TrustManager trustManager) throws CertificateException {
        // TODO somehow we need to pass the requiredcerts to this method.
        // Either we persist that state somewhere or, because we assume reconcile1 always runs first
        // we just pass it out reconcile1 and into reconcile2

        // If we got this far we can transition the trust manager to TRUSTED
        trustManager.entries().stream()
                .filter(p ->
                        p.state() == CaCertState.UNKNOWN || p.state() == CaCertState.UNTRUSTED)
                .forEach(p -> trustManager.transitionToTrusted(p.fingerprint()));

        // TODO Add the issued certs to the secrets
        // basically copy the .tmp to .crt, .key etc

        // Let the normal reconciliation restart brokers with new certs

        // TODO what about the transition to PHASE_OUT?
        // TODO what about the transition to ZOMBIE?
    }

    /**
     * Validate the given certificate chain against the given set of trust anchors.
     * To be "valid" there must be a valid cert path now and at the end of the window.
     * @param chain
     * @param trustAnchors
     * @param renewalDays
     * @throws CertificateException
     * @throws NoSuchAlgorithmException
     * @throws InvalidAlgorithmParameterException
     * @throws CertPathValidatorException
     */
    private boolean validateCertificate(List<X509Certificate> chain, Set<TrustAnchor> trustAnchors, int renewalDays) throws CertificateException {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            CertPath certPath = cf.generateCertPath(chain);

            CertPathValidator validator = CertPathValidator.getInstance("PKIX");
            PKIXParameters parameters = new PKIXParameters(trustAnchors);
            parameters.setRevocationEnabled(false);
            try {
                validator.validate(certPath, parameters);
                logger.debug("Cert path is valid currently");
            } catch (CertPathValidatorException e) {
                return false;
            }
            try {
                // TODO this isn't right
                parameters.setDate(new Date(chain.get(0).getNotBefore().getTime() + renewalDays * 1000L * 60 * 60 * 24));
                validator.validate(certPath, parameters);
                logger.debug("Cert path is still valid at the end of the renewal period");
            } catch (CertPathValidatorException e) {
                return false;
            }
            return true;
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * @return a TrustManager with state loaded from the Secret for the given cluster.
     * @throws CertificateException
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
        Pattern pattern = Pattern.compile("(?<fingerprint>[0-9A-Z]{40}).(?<state>ZOMBIE|PHASE_OUT|TRUSTED|UNSTRUSTED).crt", Pattern.CASE_INSENSITIVE);
        // load certs from the Secret values
        for (Map.Entry<String, String> e : data.entrySet()) {
            Matcher matcher = pattern.matcher(e.getKey());
            if (matcher.matches()) {
                CaCertState state = CaCertState.valueOf(matcher.group("state"));
                Fingerprint loaded = trustManager.load(e.getValue(), state);
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
            Fingerprint fp;
            if (entry.getKey().equals("ca.crt")) {
                fp = trustManager.load(entry.getValue(), CaCertState.TRUSTED);
            } else if (entry.getKey().endsWith(".crt")) {
                fp = trustManager.load(entry.getValue(), CaCertState.PHASE_OUT);
            } else {
                continue;
            }
            sb.addToData(fp.toString(), entry.getValue());
        }
        client.secrets().createOrReplace(sb.build());

        // TODO check that we don't need to need to update the p12? Or maybe we should just regenerate it.

        // update the status
        updateStatus(cr, trustManager.asStatusList());
        return trustManager;
    }

    /** Issues a client (i.e. leaf) certificate and key, storing the key and cert chain in the relevant secret as .tmp files for consumption by the 2nd half. */
    public boolean issueClientCertificate(Cr cr, TrustManager trustManager,
                                          RequiredCertificate requiredCertificate, int requestedValidity)
            throws CertificateException {
        CertificateIssuer issuer = getIssuer(cr);

        // Issue a certificate
        Subject subject = requiredCertificate.getSubject();
        IssuedCertificate issuedCert = issuer.issue(subject, requestedValidity);
        X509Certificate issuedCertificate = issuedCert.issuedCertificate();
        if (!issuedCertificate.getSubjectX500Principal().equals(new X500Principal("CN=" + subject.commonName() + ", O=" + subject.organizationName()))) {
            throw new IllegalStateException("Requested certificate for " + subject.toString() + " but was issued a certificate for " + issuedCertificate.getSubjectDN());
        }
//        issuedCert.getIssuedCertificate().getSubjectX500Principal().getName(X500Principal.CANONICAL)
        if (issuedCertificate.getSubjectAlternativeNames().equals(subject.subjectAltNames().values())) {
            throw new IllegalStateException("Requested certificate with subject alternative names" + subject.subjectAltNames().values() + " but was issued a certificate with " + issuedCertificate.getSubjectAlternativeNames());
        }

        // TODO We need to store the key and cert chain somewhere (in the CA state Secret?)
        switch (maybeEstablishTrust(cr, trustManager, requiredCertificate, issuedCert)) {
            case UNKNOWN:
            case UNTRUSTED:
                storePending(requiredCertificate, issuedCert);
                return true;
            case TRUSTED:
                storePending(requiredCertificate, issuedCert);
                return false;
            case PHASE_OUT:
            case ZOMBIE:
            default:
                throw new RuntimeException();
        }


        // TODO We make the certificate TRUSTED in the trust manager and update the CR status
        // TODO We can now update the client Secret and roll to trigger stuff to use it.

        // TODO is this actually two methods? Or a single method which results in 3 successive reconciliations?
    }

    CaCertState maybeEstablishTrust(Cr cr, TrustManager trustManager, RequiredCertificate requiredCertificate,
                             IssuedCertificate issuedCert) throws CertificateException {
        X509Certificate rootCaCertificate = issuedCert.rootCaCertificate();
        CaCertState state = trustManager.state(fingerprint(rootCaCertificate));
        switch (state) {
            case UNKNOWN:
                // It's a new CA cert, or a new CA, or a new cluster
                Secret secret = trustSecretRef().get();
                if (secret == null) {

                } else {
                    secret.getData().put(requiredCertificate.getSubject() + "something", "some state");
                    trustSecretRef().createOrReplace(secret);
                }

                // We add the certificate to trust to the trust store in the init state.

                trustManager.load(rootCaCertificate, CaCertState.UNTRUSTED);
                // TODO update CR status and the Secret
                storeTrustState(trustManager);
                updateStatus(cr, trustManager.asStatusList());
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

    /**
     * Stores an issued certificate chain until the trust relationship is established
     * @param requiredCertificate
     * @param issuedCert
     */
    private void storePending(RequiredCertificate requiredCertificate, IssuedCertificate issuedCert) {
        try {
            Resource<Secret> secretResource = client.secrets().inNamespace(namespace).withName(requiredCertificate.getSecretName());

            byte[] src = issuedCert.toPkcs12(("hello " + cluster).toCharArray());
            Secret instance = secretResource.get();

            String value = Base64.getEncoder().encodeToString(src);
            Secret build = (instance == null ? new SecretBuilder().withNewMetadata().withNamespace(namespace).withName(requiredCertificate.getSecretName()).endMetadata() : new SecretBuilder(instance))
                    .addToData(
                            requiredCertificate.getCertChainEntryName() + ".tmp",
                            value)
                    .build();
            secretResource.createOrReplace(build);
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Instantiates a CertificateIssuer according to the spec in the given CR */
    private CertificateIssuer getIssuer(Cr cr) throws CertificateException {
        // get the CA issuance options from the CR spec
        // Instantiate a corresponding impl
        CertificateIssuer issuer = null;
        if (ca(cr).isGenerateCertificateAuthority()) {


            Secret caSecret = caSecretRef().get();
            if (caSecret == null || caSecret.getData() == null || caSecret.getData().isEmpty()) {
                logger.debug("Missing CA secret: {}", caSecretName);
                // Generate a new one
                //   The Strimzi impl may, by side-effect, create a key pair if one doesn't exit
                Subject subject = new Subject();
                subject.setCommonName("cluster CA");
                subject.setOrganizationName("Strimzi");
                int validityDays = ca(cr).getValidityDays();
                StrimziCertificateIssuer sissuer = StrimziCertificateIssuer.createRootIssuer(certManager, subject, validityDays);
                // TODO For the StrimziCert issuer we need to store the key in the secret
                // (the cert will be stored later)

                Fingerprint fingerprint = fingerprint(x509Certificate(sissuer.issuingCert()));

                logger.debug("Creating CA Secret: {}", caSecretName);
                caSecret = new SecretBuilder().withNewMetadata()
                            .withName(caSecretName)
                            .withNamespace(namespace)
                        .endMetadata()
                        .addToData(fingerprint + ".key", Base64.getEncoder().encodeToString(sissuer.issuingKey()))
                        .build();
                caSecretRef().createOrReplace(caSecret);
                issuer = sissuer;
            } else {
                logger.debug("Using existing CA secret: {}", caSecretName);
                // TODO which certificate should I use??? obviously the trusted one, if there is one
                Secret trustSecret = trustSecretRef().get();
                issuer = findIssuer(trustSecret, caSecret, ".TRUSTED.crt");
                if (issuer == null) {
                    issuer = findIssuer(trustSecret, caSecret, ".UNTRUSTED.crt");
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

    private CertificateIssuer findIssuer(Secret trustSecret, Secret caSecret, String suffix) throws CertificateException {
        for (Map.Entry<String, String> entry : trustSecret.getData().entrySet()) {

            if (entry.getKey().endsWith(suffix)) {
                // Use the existing key
                byte[] caKey = Base64.getDecoder().decode(caSecret.getData().get(entry.getKey().replace(suffix, "") + ".key"));
                byte[] caCert = Base64.getDecoder().decode(entry.getValue());
                return new StrimziCertificateIssuer(certManager, new CertAndKey(caKey, caCert), List.of(x509Certificate(caCert)));
                // TODO validate that the key is a key for the issuing cert in the trust manager

            }
        }
        return null;
    }

    /**
     * Persist the state of the trust manager to the cert secret
     * @param trustManager
     */
    private void storeTrustState(TrustManager trustManager) {
        SecretBuilder secretBuilder = new SecretBuilder()
                .editOrNewMetadata()
                    .withName(trustSecretName)
                    .withNamespace(namespace)
                .endMetadata();

        for (TrustManager.CaState state : trustManager.entries()) {
            try {
                secretBuilder.addToData(state.fingerprint() + "." + state.state() + ".crt",
                        Base64.getEncoder().encodeToString(state.asPem().getBytes(StandardCharsets.US_ASCII)));
            } catch (CertificateEncodingException e) {
                throw new RuntimeException("Could not encode certificate: " + state.fingerprint(), e);
            }
        }

        trustSecretRef().createOrReplace(secretBuilder.build());
    }

    /**
     * Update the CR status with the state of the trust manager
     */
    protected abstract Cr updateStatus(Cr cr, List<CaCertStatus> statusList);
}
