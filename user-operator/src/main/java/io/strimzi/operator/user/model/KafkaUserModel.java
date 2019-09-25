/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.user.UserOperatorConfig;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.common.PasswordGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaUserModel {
    private static final Logger log = LogManager.getLogger(KafkaUserModel.class.getName());

    public static final String KEY_PASSWORD = "password";

    protected final String namespace;
    protected final String name;
    protected final Labels labels;

    protected KafkaUserAuthentication authentication;
    protected String caCert;
    protected CertAndKey userCertAndKey;
    protected String scramSha512Password;

    protected Set<SimpleAclRule> simpleAclRules = null;
    public static final String ENV_VAR_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    public static final String ENV_VAR_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";

    // Owner Reference information
    private String ownerApiVersion;
    private String ownerKind;
    private String ownerUid;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param name   User name
     * @param labels   Labels
     */
    protected KafkaUserModel(String namespace, String name, Labels labels) {
        this.namespace = namespace;
        this.name = name;
        this.labels = labels;
    }

    /**
     * Creates instance of KafkaUserModel from CRD definition.
     *
     * @param certManager CertManager instance for work with certificates.
     * @param passwordGenerator A password generator.
     * @param kafkaUser The Custom Resource based on which the model should be created.
     * @param clientsCaCert The clients CA certificate Secret.
     * @param clientsCaKey The clients CA key Secret.
     * @param userSecret Kubernetes secret with existing user certificate.
     * @return The user model.
     */
    public static KafkaUserModel fromCrd(CertManager certManager,
                                         PasswordGenerator passwordGenerator,
                                         KafkaUser kafkaUser,
                                         Secret clientsCaCert,
                                         Secret clientsCaKey,
                                         Secret userSecret) {
        KafkaUserModel result = new KafkaUserModel(kafkaUser.getMetadata().getNamespace(),
                kafkaUser.getMetadata().getName(),
                Labels.fromResource(kafkaUser).withKind(kafkaUser.getKind()));
        result.setOwnerReference(kafkaUser);
        result.setAuthentication(kafkaUser.getSpec().getAuthentication());

        if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication) {
            if (kafkaUser.getMetadata().getName().length() > OpenSslCertManager.MAXIMUM_CN_LENGTH)    {
                throw new InvalidResourceException("Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.");
            }

            result.maybeGenerateCertificates(certManager, clientsCaCert, clientsCaKey, userSecret,
                    UserOperatorConfig.getClientsCaValidityDays(), UserOperatorConfig.getClientsCaRenewalDays());
        } else if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication) {
            result.maybeGeneratePassword(passwordGenerator, userSecret);
        }

        if (kafkaUser.getSpec().getAuthorization() != null && kafkaUser.getSpec().getAuthorization().getType().equals(KafkaUserAuthorizationSimple.TYPE_SIMPLE)) {
            KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) kafkaUser.getSpec().getAuthorization();
            result.setSimpleAclRules(simple.getAcls());
        }

        return result;
    }

    /**
     * Generates secret containing the certificate for TLS client auth when TLS client auth is enabled for this user.
     * Returns null otherwise.
     *
     * @return The secret.
     */
    public Secret generateSecret()  {
        if (authentication instanceof KafkaUserTlsClientAuthentication) {
            Map<String, String> data = new HashMap<>();
            data.put("ca.crt", caCert);
            data.put("user.key", userCertAndKey.keyAsBase64String());
            data.put("user.crt", userCertAndKey.certAsBase64String());
            return createSecret(data);
        } else if (authentication instanceof KafkaUserScramSha512ClientAuthentication) {
            Map<String, String> data = new HashMap<>();
            data.put(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(scramSha512Password.getBytes(Charset.forName("US-ASCII"))));
            return createSecret(data);
        } else {
            return null;
        }
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param certManager CertManager instance for handling certificates creation
     * @param clientsCaCertSecret The clients CA certificate Secret.
     * @param clientsCaKeySecret The clients CA key Secret.
     * @param userSecret Secret with the user certificate
     * @param validityDays The number of days the certificate should be valid for.
     * @param renewalDays The renewal days.
     */
    public void maybeGenerateCertificates(CertManager certManager,
                                          Secret clientsCaCertSecret, Secret clientsCaKeySecret,
                                          Secret userSecret, int validityDays, int renewalDays) {
        if (clientsCaCertSecret == null) {
            throw new NoCertificateSecretException("The Clients CA Cert Secret is missing");
        } else if (clientsCaKeySecret == null) {
            throw new NoCertificateSecretException("The Clients CA Key Secret is missing");
        } else {
            ClientsCa clientsCa = new ClientsCa(certManager,
                    clientsCaCertSecret.getMetadata().getName(),
                    clientsCaCertSecret,
                    clientsCaCertSecret.getMetadata().getName(),
                    clientsCaKeySecret,
                    validityDays,
                    renewalDays,
                    false,
                    null);
            this.caCert = clientsCa.currentCaCertBase64();
            if (userSecret != null) {
                // Secret already exists -> lets verify if it has keys from the same CA
                String originalCaCrt = clientsCaCertSecret.getData().get("ca.crt");
                String caCrt = userSecret.getData().get("ca.crt");
                String userCrt = userSecret.getData().get("user.crt");
                String userKey = userSecret.getData().get("user.key");
                if (originalCaCrt != null
                        && originalCaCrt.equals(caCrt)
                        && userCrt != null
                        && !userCrt.isEmpty()
                        && userKey != null
                        && !userKey.isEmpty()) {
                    this.userCertAndKey = new CertAndKey(
                            decodeFromSecret(userSecret, "user.key"),
                            decodeFromSecret(userSecret, "user.crt"));
                    return;
                }
            }

            try {
                this.userCertAndKey = clientsCa.generateSignedCert(name);
            } catch (IOException e) {
                log.error("Error generating signed certificate for user {}", name, e);
            }

        }
    }

    /**
     * @param generator The password generator.
     * @param userSecret The Secret containing any existing password.
     */
    public void maybeGeneratePassword(PasswordGenerator generator, Secret userSecret) {
        if (userSecret != null) {
            // Secret already exists -> lets verify if it has a password
            String password = userSecret.getData().get(KEY_PASSWORD);
            if (password != null && !password.isEmpty()) {
                this.scramSha512Password = new String(Base64.getDecoder().decode(password), Charset.forName("US-ASCII"));
                return;
            }
        }
        log.debug("Generating user password");
        this.scramSha512Password = generator.generate();

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
     * @param data Map with the Secret content
     * @return The secret.
     */
    protected Secret createSecret(Map<String, String> data) {
        Secret s = new SecretBuilder()
                .withNewMetadata()
                    .withName(getSecretName())
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
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
            throw new RuntimeException("At least one authentication mechanism has to be selected");
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
     * @param username The username.
     * @return The name of the user.
     */
    public static String getSecretName(String username)    {
        return username;
    }

    /**
     * Gets the name of the User secret.
     *
     * @return The name of the user secret.
     */
    public String getSecretName()    {
        return KafkaUserModel.getSecretName(name);
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
}
