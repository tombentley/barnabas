/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStoreException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.Clock;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.pki.InternalCertificateIssuer;
import io.strimzi.operator.cluster.model.pki.IssuedCertificate;
import io.strimzi.operator.cluster.model.pki.PkiUtils;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

public class KafkaUserModel {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaUserModel.class.getName());

    public static final String KEY_PASSWORD = "password";
    public static final String KEY_SASL_JAAS_CONFIG = "sasl.jaas.config";

    private final Clock clock = Clock.systemUTC();

    protected final String namespace;
    protected final String name;
    protected final Labels labels;

    protected KafkaUserAuthentication authentication;

    protected Set<SimpleAclRule> simpleAclRules = null;
    public static final String ENV_VAR_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    public static final String ENV_VAR_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";

    public static final String KAFKA_USER_OPERATOR_NAME = "strimzi-user-operator";

    // Owner Reference information
    private String ownerApiVersion;
    private String ownerKind;
    private String ownerUid;

    private KafkaUserQuotas quotas;
    private Map<String, String> templateSecretLabels;
    private Map<String, String> templateSecretAnnotations;

    private final String secretPrefix;
    private Secret userSecret;

    /**
     * Constructor
     *
     * @param namespace Kubernetes namespace where Kafka Connect cluster resources are going to be created
     * @param name   User name
     * @param labels   Labels
     */
    protected KafkaUserModel(String namespace, String name, Labels labels, String secretPrefix) {
        this.namespace = namespace;
        this.name = name;
        this.labels = labels.withKubernetesName(KAFKA_USER_OPERATOR_NAME)
            .withKubernetesInstance(name)
            .withKubernetesPartOf(name)
            .withKubernetesManagedBy(KAFKA_USER_OPERATOR_NAME);
        this.secretPrefix = secretPrefix;
    }

    /**
     * Creates instance of KafkaUserModel from CRD definition.
     *
     * @param reconciliation The reconciliation
     * @param certManager CertManager instance for work with certificates.
     * @param passwordGenerator A password generator.
     * @param kafkaUser The Custom Resource based on which the model should be created.
     * @param clientsCaCert The clients CA certificate Secret.
     * @param clientsCaKey The clients CA key Secret.
     * @param userSecret Kubernetes secret with existing user certificate.
     * @param secretPrefix The prefix used to add to the name of the Secret generated from the KafkaUser resource.
     * @return The user model.
     */
    public static KafkaUserModel fromCrd(Reconciliation reconciliation,
                                         CertManager certManager,
                                         PasswordGenerator passwordGenerator,
                                         KafkaUser kafkaUser,
                                         Secret clientsCaCert,
                                         Secret clientsCaKey,
                                         Secret userSecret, String secretPrefix) {
        KafkaUserModel result = new KafkaUserModel(kafkaUser.getMetadata().getNamespace(),
                kafkaUser.getMetadata().getName(),
                Labels.fromResource(kafkaUser).withStrimziKind(kafkaUser.getKind()),
                secretPrefix);
        result.setOwnerReference(kafkaUser);
        result.setAuthentication(kafkaUser.getSpec().getAuthentication());

        if (kafkaUser.getSpec().getTemplate() != null
                && kafkaUser.getSpec().getTemplate().getSecret() != null
                && kafkaUser.getSpec().getTemplate().getSecret().getMetadata() != null)  {
            result.templateSecretLabels = kafkaUser.getSpec().getTemplate().getSecret().getMetadata().getLabels();
            result.templateSecretAnnotations = kafkaUser.getSpec().getTemplate().getSecret().getMetadata().getAnnotations();
        }

        if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication) {
            if (kafkaUser.getMetadata().getName().length() > OpenSslCertManager.MAXIMUM_CN_LENGTH)    {
                throw new InvalidResourceException("Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.");
            }

            result.userSecret = result.maybeGenerateCertificates(reconciliation, certManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret,
                    UserOperatorConfig.getClientsCaValidityDays(), UserOperatorConfig.getClientsCaRenewalDays());
        } else if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication) {
            result.userSecret = result.maybeGeneratePassword(reconciliation, passwordGenerator, userSecret);
        } else {
            result.userSecret = null;
        }

        if (kafkaUser.getSpec().getAuthorization() != null && kafkaUser.getSpec().getAuthorization().getType().equals(KafkaUserAuthorizationSimple.TYPE_SIMPLE)) {
            KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) kafkaUser.getSpec().getAuthorization();
            result.setSimpleAclRules(simple.getAcls());
        }
        result.setQuotas(kafkaUser.getSpec().getQuotas());

        return result;
    }

    private Secret scramShaSecret(String scramSha512Password) {
        Map<String, String> data = new HashMap<>(2);
        data.put(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(scramSha512Password.getBytes(StandardCharsets.US_ASCII)));
        data.put(KafkaUserModel.KEY_SASL_JAAS_CONFIG, Base64.getEncoder().encodeToString(getSaslJaasConfig(scramSha512Password).getBytes(StandardCharsets.US_ASCII)));
        return createSecret(data);
    }

    private Secret tlsSecret(String caCert, CertAndKey userCertAndKey) {
        Map<String, String> data = new HashMap<>(5);
        data.put("ca.crt", caCert);
        data.put("user.key", userCertAndKey.keyAsBase64String());
        data.put("user.crt", userCertAndKey.certAsBase64String());
        data.put("user.p12", userCertAndKey.keyStoreAsBase64String());
        data.put("user.password", userCertAndKey.storePasswordAsBase64String());
        return createSecret(data);
    }

    private boolean isExpiring(String userCrt, int renewalDays) throws CertificateException {
        X509Certificate x509 = PkiUtils.x509Certificate(Base64.getDecoder().decode(userCrt));
        Date notAfter = x509.getNotAfter();
        long msTillExpired = notAfter.getTime() - clock.millis();
        return msTillExpired < renewalDays * 24L * 60L * 60L * 1000L;
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param reconciliation The reconciliation
     * @param certManager CertManager instance for handling certificates creation
     * @param passwordGenerator PasswordGenerator instance for generating passwords
     * @param clientsCaCertSecret The clients CA certificate Secret.
     * @param clientsCaKeySecret The clients CA key Secret.
     * @param userSecret Secret with the user certificate
     * @param validityDays The number of days the certificate should be valid for.
     * @param renewalDays The renewal days.
     * @return The user Secret containing the user's TLS certificate and key (in PEM and PKCS#12 with password)
     * together with the client's CA certificate used to sign the user certificate.
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public Secret maybeGenerateCertificates(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator,
                                            Secret clientsCaCertSecret, Secret clientsCaKeySecret,
                                            Secret userSecret, int validityDays, int renewalDays) {
        if (clientsCaCertSecret == null) {
            throw new NoCertificateSecretException("The Clients CA Cert Secret is missing");
        } else if (clientsCaKeySecret == null) {
            throw new NoCertificateSecretException("The Clients CA Key Secret is missing");
        } else {
            try {
                byte[] issuingKey = Base64.getDecoder().decode(clientsCaKeySecret.getData().get("ca.key"));
                final String caCert = clientsCaCertSecret.getData().get("ca.crt");

                Subject subject = new Subject.Builder().withCommonName(name).build();

                CertAndKey userCertAndKey = null;
                if (userSecret != null) {
                    // Secret already exists -> lets verify if it has keys from the same CA
                    String originalCaCrt = clientsCaCertSecret.getData().get("ca.crt");
                    String caCrt = userSecret.getData().get("ca.crt");
                    String userCrt = userSecret.getData().get("user.crt");
                    String userKey = userSecret.getData().get("user.key");
                    String userKeyStore = userSecret.getData().get("user.p12");
                    String userKeyStorePassword = userSecret.getData().get("user.password");
                    if (originalCaCrt != null
                            && originalCaCrt.equals(caCrt)
                            && userCrt != null
                            && !userCrt.isEmpty()
                            && userKey != null
                            && !userKey.isEmpty()
                            && !isExpiring(userCrt, renewalDays)) {

                        if (userKeyStore != null
                                && !userKeyStore.isEmpty()
                                && userKeyStorePassword != null
                                && !userKeyStorePassword.isEmpty()) {

                            userCertAndKey = new CertAndKey(
                                    decodeFromSecret(userSecret, "user.key"),
                                    decodeFromSecret(userSecret, "user.crt"),
                                    null,
                                    decodeFromSecret(userSecret, "user.p12"),
                                    new String(decodeFromSecret(userSecret, "user.password"), StandardCharsets.US_ASCII));
                        } else {
                            // coming from an older operator version, the user secret exists but without keystore and password
                            try {
                                String keystorePassword = passwordGenerator.generate();
                                CertAndKey result = new CertAndKey(
                                        decodeFromSecret(userSecret, "user.key"),
                                        decodeFromSecret(userSecret, "user.crt"),
                                        null,
                                        new IssuedCertificate(decodeFromSecret(userSecret, "user.key"),
                                                PkiUtils.x509Certificate(decodeFromSecret(userSecret, "user.crt")),
                                                List.of(),
                                                PkiUtils.x509Certificate(decodeFromSecret(clientsCaCertSecret, "ca.crt"))).toPkcs12(keystorePassword.toCharArray()),
                                        keystorePassword);
//                                userCertAndKey = clientsCa.addKeyAndCertToKeyStore(name,
//                                        decodeFromSecret(userSecret, "user.key"),
//                                        decodeFromSecret(userSecret, "user.crt"));
                            } catch (IOException | KeyStoreException | InvalidKeySpecException e) {
                                LOGGER.errorCr(reconciliation, "Error generating the keystore for user {}", name, e);
                            }
                        }
                    } else {
                        try {
                            X509Certificate x509Certificate = PkiUtils.x509Certificate(Base64.getDecoder().decode(caCert));
                            userCertAndKey = generateSignedCert(reconciliation, certManager, validityDays, issuingKey, x509Certificate, subject);
                        } catch (CertificateException | KeyStoreException | IOException | InvalidKeySpecException e) {
                            throw new RuntimeException("Could not parse or issue certificate using ca.crt in " + clientsCaCertSecret.getMetadata().getName());
                        }
                    }
                } else {
                    try {
                        X509Certificate x509Certificate = PkiUtils.x509Certificate(Base64.getDecoder().decode(caCert));
                        userCertAndKey = generateSignedCert(reconciliation, certManager, validityDays, issuingKey, x509Certificate, subject);
                    } catch (CertificateException | KeyStoreException | IOException | InvalidKeySpecException e) {
                        throw new RuntimeException("Could not parse or issue certificate using ca.crt in " + clientsCaCertSecret.getMetadata().getName());
                    }
                }
                return tlsSecret(caCert, userCertAndKey);
            } catch (CertificateException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected CertAndKey generateSignedCert(Reconciliation reconciliation, CertManager certManager, int validityDays,
                                            byte[] issuingKey, X509Certificate issuingCert, Subject subject) throws CertificateException, KeyStoreException, IOException, InvalidKeySpecException {
        CertAndKey userCertAndKey;
        InternalCertificateIssuer issuer = new InternalCertificateIssuer(reconciliation, clock, certManager, List.of(issuingCert), issuingKey, issuingCert);
        byte[] tokenBytes = issuer.requestCertificate(subject, validityDays);
        Optional<IssuedCertificate> issuedOpt = issuer.pollForRequestedCertificate(tokenBytes);
        // The interface is designed to accommodate asynchrony between request and issuance
        // but we know the InternalCertificateIssuer will always be able to complete the request
        // on first poll.
        IssuedCertificate issued = issuedOpt.orElseThrow();
        String keystorePassword = "1234";
        userCertAndKey = new CertAndKey(
                issued.privateKey(),
                issued.issuedCertBytes(),
                null,
                new IssuedCertificate(issued.privateKey(),
                        issued.issuedCertificate(),
                        List.of(issuingCert),
                        issuingCert).toPkcs12(keystorePassword.toCharArray()),
                keystorePassword);
        return userCertAndKey;
    }

    /**
     * @param reconciliation The reconciliation.
     * @param generator The password generator.
     * @param userSecret The Secret containing any existing password.
     * @return The user Secret containing the password
     */
    public Secret maybeGeneratePassword(Reconciliation reconciliation, PasswordGenerator generator, Secret userSecret) {
        final String scramSha512Password;
        if (userSecret != null) {
            // Secret already exists -> lets verify if it has a password
            String password = userSecret.getData().get(KEY_PASSWORD);
            if (password != null && !password.isEmpty()) {
                scramSha512Password = new String(Base64.getDecoder().decode(password), StandardCharsets.US_ASCII);
            } else {
                LOGGER.debugCr(reconciliation, "Generating user password");
                scramSha512Password = generator.generate();
            }
        } else {
            LOGGER.debugCr(reconciliation, "Generating user password");
            scramSha512Password = generator.generate();
        }
        return scramShaSecret(scramSha512Password);

    }

    /**
     * Decode from Base64 a keyed value from a Secret
     *
     * @param secret Secret from which decoding the value
     * @param key Key of the value to decode
     * @return decoded value
     */
    protected byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Creates secret with the data
     *
     * @param data Map with the Secret content
     * @return The secret.
     */
    protected Secret createSecret(Map<String, String> data) {
        Secret s = new SecretBuilder()
                .withNewMetadata()
                    .withName(getSecretName())
                    .withNamespace(namespace)
                    .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), templateSecretLabels))
                    .withAnnotations(Util.mergeLabelsOrAnnotations(null, templateSecretAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();

        return s;
    }

    /**
     * Generate the OwnerReference object to link newly created objects to their parent (the custom resource)
     *
     * @return The owner reference.
     */
    protected OwnerReference createOwnerReference() {
        return new OwnerReferenceBuilder()
                .withApiVersion(ownerApiVersion)
                .withKind(ownerKind)
                .withName(name)
                .withUid(ownerUid)
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
    }

    /**
     * Set fields needed to generate the OwnerReference object
     *
     * @param parent The resource which should be used as parent. It will be used to gather the date needed for generating OwnerReferences.
     */
    protected void setOwnerReference(HasMetadata parent)  {
        this.ownerApiVersion = parent.getApiVersion();
        this.ownerKind = parent.getKind();
        this.ownerUid = parent.getMetadata().getUid();
    }

    /**
     * Decodes the name of the User secret based on the username
     *
     * @param username The username.
     * @return The decoded user name.
     */
    public static String decodeUsername(String username) {
        if (username.contains("CN="))   {
            try {
                return new LdapName(username).getRdns().stream()
                        .filter(rdn -> rdn.getType().equalsIgnoreCase("cn"))
                        .map(rdn -> rdn.getValue().toString()).collect(Collectors.joining());
            } catch (InvalidNameException e)    {
                throw new IllegalArgumentException(e);
            }
        } else  {
            return username;
        }
    }

    /**
     * Generates the name of the User secret based on the username
     *
     * @param username The username.
     * @return The TLS user name.
     */
    public static String getTlsUserName(String username)    {
        return "CN=" + username;
    }

    /**
     * Generates the name of the User secret based on the username
     *
     * @param username The username.
     * @return The SCRAM user name.
     */
    public static String getScramUserName(String username)    {
        return username;
    }

    /**
     * Gets the Username
     *
     * @return The user name.
     */
    public String getUserName()    {
        if (isTlsUser()) {
            return getTlsUserName(name);
        } else if (isScramUser()) {
            return getScramUserName(name);
        } else {
            return getName();
        }
    }

    /**
     * @return The name of the user.
     */
    public String getName() {
        return name;
    }

    /**
     * Generates the name of the User secret based on the username.
     *
     * @param secretPrefix The secret prefix
     * @param username The username.
     * @return The name of the user.
     */
    public static String getSecretName(String secretPrefix, String username)    {
        return secretPrefix + username;
    }

    /**
     * Creates the JAAS configuration string for SASL SCRAM-SHA-512 authentication.
     *
     * @param username The SCRAM username.
     * @param password The SCRAM password.
     * @return The JAAS configuration string.
     */
    public static String getSaslJaasConfig(String username, String password) {
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
    }

    /**
     * Gets the name of the User secret.
     *
     * @return The name of the user secret.
     */
    public String getSecretName()    {
        return KafkaUserModel.getSecretName(secretPrefix, name);
    }

    /**
     * Sets the authentication method.
     *
     * @param authentication Authentication method.
     */
    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    /**
     * Sets the quotas to the user.
     *
     * @param quotas KafkaUserQuotas to be set.
     */
    public void setQuotas(KafkaUserQuotas quotas) {
        this.quotas = quotas;
    }

    /**
     * Gets the quotas.
     *
     * @return User Quotas.
     */
    public KafkaUserQuotas getQuotas()    {
        return quotas;
    }

    /**
     * Get the list of ACL rules for Simple Authorization which should apply to this user.
     *
     * @return The ACL rules.
     */
    public Set<SimpleAclRule> getSimpleAclRules() {
        return simpleAclRules;
    }

    /**
     * Sets the list of ACL rules for Simple authorization.
     *
     * @param rules List of ACL rules which should be applied to this user.
     */
    public void setSimpleAclRules(List<AclRule> rules) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<SimpleAclRule>();

        for (AclRule rule : rules)  {
            simpleAclRules.add(SimpleAclRule.fromCrd(rule));
        }

        this.simpleAclRules = simpleAclRules;
    }

    /**
     * Returns true if the user is using TLS authentication.
     *
     * @return true if the user is using TLS authentication.
     */
    public boolean isTlsUser()  {
        return authentication instanceof KafkaUserTlsClientAuthentication;
    }

    /**
     * Returns true if the user is using SCRAM-SHA-512 authentication.
     *
     * @return true if the user is using SCRAM-SHA-512 authentication.
     */
    public boolean isScramUser()  {
        return authentication instanceof KafkaUserScramSha512ClientAuthentication;
    }

    /**
     * Returns true if the user is configured without authentication section and is not using any authentication.
     * Such user might be used for example for things such as OAUTH authentication where the users are not managed by
     * the User Operator.
     *
     * @return true if the user is not using any authentication.
     */
    public boolean isNoneUser()  {
        return authentication == null;
    }

    /**
     * Creates the JAAS configuration string for SASL SCRAM-SHA-512 authentication.
     *
     * @param scramSha512Password The password
     * @return The JAAS configuration string.
     */
    public String getSaslJaasConfig(String scramSha512Password) {
        return getSaslJaasConfig(getScramUserName(name), scramSha512Password);
    }

    public Secret getUserSecret() {
        return userSecret;
    }
}
