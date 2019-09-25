/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.common.PasswordGenerator;
import org.junit.Test;

import java.util.Base64;

import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaUserModelTest {
    private final KafkaUser tlsUser = ResourceUtils.createKafkaUserTls();
    private final KafkaUser scramShaUser = ResourceUtils.createKafkaUserScramSha();
    private final Secret clientsCaCert = ResourceUtils.createClientsCaCertSecret();
    private final Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
    private final CertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    public void checkOwnerReference(OwnerReference ownerRef, HasMetadata resource)  {
        assertEquals(1, resource.getMetadata().getOwnerReferences().size());
        assertEquals(ownerRef, resource.getMetadata().getOwnerReferences().get(0));
    }

    @Test
    public void testFromCrd()   {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);

        assertEquals(ResourceUtils.NAMESPACE, model.namespace);
        assertEquals(ResourceUtils.NAME, model.name);
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND), model.labels);
        assertEquals(KafkaUserTlsClientAuthentication.TYPE_TLS, model.authentication.getType());

        KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) tlsUser.getSpec().getAuthorization();
        assertEquals(ResourceUtils.createExpectedSimpleAclRules(tlsUser).size(), model.getSimpleAclRules().size());
        assertEquals(ResourceUtils.createExpectedSimpleAclRules(tlsUser), model.getSimpleAclRules());
    }

    @Test
    public void testGenerateSecret()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);
        Secret generated = model.generateSecret();

        assertEquals(set("ca.crt", "user.crt", "user.key"), generated.getData().keySet());

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateWhenNoExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, null);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateAtCaChange()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        Secret clientsCaCertSecret = ResourceUtils.createClientsCaCertSecret();
        clientsCaCertSecret.getData().put("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));
        Secret clientsCaKeSecret = ResourceUtils.createClientsCaKeySecret();
        clientsCaKeSecret.getData().put("ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));

        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCertSecret, clientsCaKeSecret, userCert);
        Secret generated = model.generateSecret();

        assertEquals("different-clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateKeepExisting()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("expected-crt", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("expected-key", new String(model.decodeFromSecret(generated, "user.key")));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGenerateCertificateExistingScramSha()    {
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tlsUser, clientsCaCert, clientsCaKey, userCert);
        Secret generated = model.generateSecret();

        assertEquals("clients-ca-crt", new String(model.decodeFromSecret(generated, "ca.crt")));
        assertEquals("crt file", new String(model.decodeFromSecret(generated, "user.crt")));
        assertEquals("key file", new String(model.decodeFromSecret(generated, "user.key")));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordWhenNoSecretExists()    {
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, null);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton(KafkaUserModel.KEY_PASSWORD), generated.getData().keySet());
        assertEquals("aaaaaaaaaa", new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordKeepExistingScramSha()    {
        Secret userPassword = ResourceUtils.createUserSecretScramSha();
        String existing = userPassword.getData().get(KafkaUserModel.KEY_PASSWORD);
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, userPassword);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton(KafkaUserModel.KEY_PASSWORD), generated.getData().keySet());
        assertEquals(existing, generated.getData().get(KafkaUserModel.KEY_PASSWORD));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testGeneratePasswordExistingTlsSecret()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, scramShaUser, clientsCaCert, clientsCaKey, userCert);
        Secret generated = model.generateSecret();

        assertEquals(ResourceUtils.NAME, generated.getMetadata().getName());
        assertEquals(ResourceUtils.NAMESPACE, generated.getMetadata().getNamespace());
        assertEquals(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND).toMap(), generated.getMetadata().getLabels());

        assertEquals(singleton("password"), generated.getData().keySet());
        assertEquals("aaaaaaaaaa", new String(Base64.getDecoder().decode(generated.getData().get(KafkaUserModel.KEY_PASSWORD))));

        // Check owner reference
        checkOwnerReference(model.createOwnerReference(), generated);
    }

    @Test
    public void testNoTlsAuthn()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert);

        assertNull(model.generateSecret());
    }

    @Test
    public void testNoSimpleAuthz()    {
        Secret userCert = ResourceUtils.createUserSecretTls();
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.setSpec(new KafkaUserSpec());
        KafkaUserModel model = KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, user, clientsCaCert, clientsCaKey, userCert);

        assertNull(model.getSimpleAclRules());
    }

    @Test
    public void testDecodeUsername()    {
        assertEquals("my-user", KafkaUserModel.decodeUsername("CN=my-user"));
        assertEquals("my-user", KafkaUserModel.decodeUsername("CN=my-user,OU=my-org"));
        assertEquals("my-user", KafkaUserModel.decodeUsername("OU=my-org,CN=my-user"));
    }

    @Test
    public void testGetUsername()    {
        assertEquals("CN=my-user", KafkaUserModel.getTlsUserName("my-user"));
        assertEquals("my-user", KafkaUserModel.getScramUserName("my-user"));
    }

    @Test(expected = InvalidResourceException.class)
    public void test65CharTlsUsername()    {
        // 65 characters => Should throw exception with TLS
        KafkaUser tooLong = new KafkaUserBuilder(tlsUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null);
    }

    @Test
    public void test64CharTlsUsername()    {
        // 64 characters => Should be still OK
        KafkaUser notTooLong = new KafkaUserBuilder(tlsUser)
                .editMetadata()
                    .withName("User123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, notTooLong, clientsCaCert, clientsCaKey, null);
    }

    @Test
    public void test65CharSaslUsername()    {
        // 65 characters => should work with SCRAM-SHA-512
        KafkaUser tooLong = new KafkaUserBuilder(scramShaUser)
                .editMetadata()
                    .withName("User-123456789012345678901234567890123456789012345678901234567890")
                .endMetadata()
                .build();

        KafkaUserModel.fromCrd(mockCertManager, passwordGenerator, tooLong, clientsCaCert, clientsCaKey, null);
    }
}
