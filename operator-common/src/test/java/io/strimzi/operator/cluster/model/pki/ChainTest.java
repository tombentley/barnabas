/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.mockkube.MockKube;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChainTest {
    // TODO when happens when root CA is going to expire
    // TODO what happens when root CA has expired
    // TODO what happens when intermediate CA is going to expire
    // TODO what happens when intermediate CA is going to expire

    Chain chain;
    private MixedOperation<Secret, SecretList, Resource<Secret>> secrets;
    private TestClock clock;
    private String clusterName;
    private Reconciliation reconciliation = new Reconciliation("test", "Kafka", "ns", "test");

    @BeforeEach
    public void setup() {
        clock = new TestClock(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));
        KubernetesClient client = new MockKube().build();
        secrets = client.secrets();
        chain = new Chain(reconciliation, clock, secrets, new OpenSslCertManager());
    }

    public static List<CertificateAuthority> clusterCaSource() {
        return List.of(
                new CertificateAuthorityBuilder()
                        .withValidityDays(10)
                        .withRenewalDays(3)
                        .withGenerateSecretOwnerReference(true)
                        .withGenerateCertificateAuthority(true)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                        .build(),
                new CertificateAuthorityBuilder()
                        .withValidityDays(10)
                        .withRenewalDays(3)
                        .withGenerateSecretOwnerReference(true)
                        .withGenerateCertificateAuthority(false)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                        .build(),
                new CertificateAuthorityBuilder()
                        .withValidityDays(10)
                        .withRenewalDays(3)
                        .withGenerateSecretOwnerReference(false)
                        .withGenerateCertificateAuthority(true)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                        .build(),
                new CertificateAuthorityBuilder()
                        .withValidityDays(10)
                        .withRenewalDays(3)
                        .withGenerateSecretOwnerReference(false)
                        .withGenerateCertificateAuthority(false)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                        .build(),
                new CertificateAuthorityBuilder()
                        .withValidityDays(10)
                        .withRenewalDays(3)
                        .withGenerateSecretOwnerReference(true)
                        .withGenerateCertificateAuthority(true)
                        .withCertificateExpirationPolicy(CertificateExpirationPolicy.RENEW_CERTIFICATE)
                        .build()
                );
    }

    @ParameterizedTest
    @MethodSource("clusterCaSource")
    public void clusterCa(CertificateAuthority ca) throws CertificateException {
        int validityDays = ca.getValidityDays();
        assertEquals(Set.of(), secretNames());

        clusterName = "test-cluster";
        Chain.InternalIssuerSpec internalIssuerSpec = Chain.clusterCaFromCrd(clusterName, ca);

        OwnerReference ownerReference;
        if (ca.isGenerateSecretOwnerReference()) {
            ownerReference = new OwnerReferenceBuilder()
                    .withKind("Kafka")
                    .withName(clusterName)
                    .build();
        } else {
            ownerReference = null;
        }

        // Expect initial reconciliation to create the secrets
        if (ca.isGenerateCertificateAuthority()) {
            chain.reconcile(internalIssuerSpec, ownerReference);
        } else {
            PkiException pkiException = assertThrows(PkiException.class,
                () -> chain.reconcile(internalIssuerSpec, ownerReference));
            assertEquals("CA Certificate ca.crt in secret test-cluster-cluster-ca-cert does not exist and is not configured to allow automatic issuance", pkiException.getMessage());
            return;
        }
        assertEquals(Set.of("test-cluster-cluster-ca", "test-cluster-cluster-ca-cert"), secretNames());

        assertEquals(1, secrets.withName("test-cluster-cluster-ca-cert").get().getMetadata().getGeneration());
        assertEquals(Set.of("ca.crt"), secrets.withName("test-cluster-cluster-ca-cert").get().getData().keySet());
        assertOwnerReferences(ownerReference, "test-cluster-cluster-ca-cert");
        X509Certificate cert = x509FromBase64String(secrets.withName("test-cluster-cluster-ca-cert").get().getData().get("ca.crt"));
        assertEquals(Instant.ofEpochMilli(0), cert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(validityDays, DAYS), cert.getNotAfter().toInstant());
        assertEquals("CN=cluster-ca v1, O=io.strimzi", cert.getSubjectDN().toString());
        assertEquals("CN=cluster-ca v1, O=io.strimzi", cert.getIssuerDN().toString());
        assertEquals(0, cert.getBasicConstraints(), "Expected path length of 0");

        assertEquals(1, secrets.withName("test-cluster-cluster-ca").get().getMetadata().getGeneration());
        assertEquals(Set.of("ca.key"), secrets.withName("test-cluster-cluster-ca").get().getData().keySet());
        assertOwnerReferences(ownerReference, "test-cluster-cluster-ca");
        String key = secrets.withName("test-cluster-cluster-ca").get().getData().get("ca.key");
        assertNotNull(key);

        // Reconcile a day later shouldn't change anything
        clock.advanceFromOrigin(Duration.ofDays(1));
        chain.reconcile(internalIssuerSpec, ownerReference);
        assertEquals(1, secrets.withName("test-cluster-cluster-ca").get().getMetadata().getGeneration());
        assertEquals(1, secrets.withName("test-cluster-cluster-ca-cert").get().getMetadata().getGeneration());

        // Reconcile within the renewal period should change
        clock.advanceFromOrigin(Duration.ofDays(ca.getValidityDays() - ca.getRenewalDays() + 1));
        chain.reconcile(internalIssuerSpec, ownerReference);
        assertEquals(2, secrets.withName("test-cluster-cluster-ca").get().getMetadata().getGeneration());
        assertEquals(2, secrets.withName("test-cluster-cluster-ca-cert").get().getMetadata().getGeneration());
        switch (ca.getCertificateExpirationPolicy()) {
            case REPLACE_KEY:
                assertNotEquals(key, secrets.withName("test-cluster-cluster-ca").get().getData().get("ca.key"));
                break;
            case RENEW_CERTIFICATE:
                assertEquals(key, secrets.withName("test-cluster-cluster-ca").get().getData().get("ca.key"));
                break;
        }

        // Reconcile a day later shouldn't change anything
        clock.advanceFromOrigin(Duration.ofDays(ca.getValidityDays() - ca.getRenewalDays() + 2));
        chain.reconcile(internalIssuerSpec, ownerReference);
        assertEquals(2, secrets.withName("test-cluster-cluster-ca").get().getMetadata().getGeneration());
        assertEquals(2, secrets.withName("test-cluster-cluster-ca-cert").get().getMetadata().getGeneration());
    }

    public static List<Chain.InternalIssuerSpec> intermediateIssuerSource() {
        return List.of(
                new Chain.InternalIssuerSpec(3,
                        List.of(
                                new Chain.CaCertificateReference("foo", "root.crt",
                                "foo", "root.key",
                                new Chain.IssuanceParams(
                                        new Subject.Builder().withCommonName("root").build(), 20,
                                        CertificateExpirationPolicy.REPLACE_KEY)),
                                new Chain.CaCertificateReference("foo", "intermediate.crt",
                                "foo", "intermediate.key",
                                new Chain.IssuanceParams(
                                        new Subject.Builder().withCommonName("intermediate").build(), 10,
                                        CertificateExpirationPolicy.REPLACE_KEY))
                        )
                )
        );
    }
    @ParameterizedTest
    @MethodSource("intermediateIssuerSource")
    public void intermediateIssuer(Chain.InternalIssuerSpec spec) throws CertificateException {
        assertEquals(2, spec.getChain().size());
        OwnerReference ownerReference = null;
        int rootValidityDays = spec.getChain().get(0).getIssuanceParams().getValidity();
        int intermediateValidityDays = spec.getChain().get(1).getIssuanceParams().getValidity();
        chain.reconcile(spec, ownerReference);
        assertEquals(Set.of("foo"), secretNames());

        assertEquals(2, secrets.withName("foo").get().getMetadata().getGeneration());
        assertEquals(Set.of("root.crt", "root.key", "intermediate.crt", "intermediate.key"), secrets.withName("foo").get().getData().keySet());
        assertOwnerReferences(ownerReference, "foo");
        X509Certificate rootCert = x509FromBase64String(secrets.withName("foo").get().getData().get("root.crt"));
        assertEquals(Instant.ofEpochMilli(0), rootCert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(rootValidityDays, DAYS), rootCert.getNotAfter().toInstant());
        assertEquals("CN=root", rootCert.getSubjectDN().toString());
        assertEquals("CN=root", rootCert.getIssuerDN().toString());
        assertEquals(1, rootCert.getBasicConstraints(), "Expected path length of 1");

        X509Certificate intermediateCert = x509FromBase64String(secrets.withName("foo").get().getData().get("intermediate.crt"));
        assertEquals(Instant.ofEpochMilli(0), intermediateCert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(intermediateValidityDays, DAYS), intermediateCert.getNotAfter().toInstant());
        assertEquals("CN=intermediate", intermediateCert.getSubjectDN().toString());
        assertEquals("CN=root", intermediateCert.getIssuerDN().toString());
        assertEquals(0, intermediateCert.getBasicConstraints(), "Expected path length of 0");

        String rootKey = secrets.withName("foo").get().getData().get("root.key");
        assertNotNull(rootKey);
        String intermediateKey = secrets.withName("foo").get().getData().get("intermediate.key");
        assertNotNull(intermediateKey);

        // Reconcile a day later shouldn't change anything
        clock.advanceFromOrigin(Duration.ofDays(1));
        chain.reconcile(spec, ownerReference);
        assertEquals(2, secrets.withName("foo").get().getMetadata().getGeneration());
        assertEquals(2, secrets.withName("foo").get().getMetadata().getGeneration());

        // Reconcile within the renewal period should change
        clock.advanceFromOrigin(Duration.ofDays(intermediateValidityDays - spec.getRenewalDays() + 1));
        chain.reconcile(spec, ownerReference);
        assertEquals(3, secrets.withName("foo").get().getMetadata().getGeneration());
        assertEquals(rootCert, x509FromBase64String(secrets.withName("foo").get().getData().get("root.crt")),
                "expected the root cert not to have changed");
        assertEquals(rootKey, secrets.withName("foo").get().getData().get("root.key"),
                "expected the root key not to have changed");
        assertNotEquals(intermediateCert, x509FromBase64String(secrets.withName("foo").get().getData().get("intermediate.crt")),
                "expected the intermediate cert to have changed");
        assertNotEquals(intermediateKey, secrets.withName("foo").get().getData().get("intermediate.key"),
                "expected the intermediate key to have changed");
        intermediateCert = x509FromBase64String(secrets.withName("foo").get().getData().get("intermediate.crt"));
        assertEquals(Instant.ofEpochMilli(0).plus(intermediateValidityDays - spec.getRenewalDays() + 1, DAYS), intermediateCert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(intermediateValidityDays - spec.getRenewalDays() + 1 + intermediateValidityDays, DAYS), intermediateCert.getNotAfter().toInstant());
        assertEquals("CN=intermediate", intermediateCert.getSubjectDN().toString());
        assertEquals("CN=root", intermediateCert.getIssuerDN().toString());
        assertEquals(0, intermediateCert.getBasicConstraints(), "Expected path length of 0");


        // New reconcile in the renewal period of the root ca
        clock.advanceFromOrigin(Duration.ofDays(rootValidityDays - spec.getRenewalDays() + 1));
        chain.reconcile(spec, ownerReference);
        assertEquals(5, secrets.withName("foo").get().getMetadata().getGeneration());
        assertNotEquals(rootCert, x509FromBase64String(secrets.withName("foo").get().getData().get("root.crt")),
                "expected the root cert to have changed");
        assertNotEquals(rootKey, secrets.withName("foo").get().getData().get("root.key"),
                "expected the root key to have changed");
        rootCert = x509FromBase64String(secrets.withName("foo").get().getData().get("root.crt"));
        assertEquals(Instant.ofEpochMilli(0).plus(rootValidityDays - spec.getRenewalDays() + 1, DAYS), rootCert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(rootValidityDays - spec.getRenewalDays() + 1 + rootValidityDays, DAYS), rootCert.getNotAfter().toInstant());
        assertEquals("CN=root", rootCert.getSubjectDN().toString());
        assertEquals("CN=root", rootCert.getIssuerDN().toString());
        assertEquals(1, rootCert.getBasicConstraints(), "Expected path length of 1");

        assertNotEquals(intermediateCert, x509FromBase64String(secrets.withName("foo").get().getData().get("intermediate.crt")),
                "expected the intermediate cert to have changed");
        assertNotEquals(intermediateKey, secrets.withName("foo").get().getData().get("intermediate.key"),
                "expected the intermediate key to have changed");
        intermediateCert = x509FromBase64String(secrets.withName("foo").get().getData().get("intermediate.crt"));
        assertEquals(Instant.ofEpochMilli(0).plus(rootValidityDays - spec.getRenewalDays() + 1, DAYS), intermediateCert.getNotBefore().toInstant());
        assertEquals(Instant.ofEpochMilli(0).plus(rootValidityDays - spec.getRenewalDays() + 1 + intermediateValidityDays, DAYS), intermediateCert.getNotAfter().toInstant());
        assertEquals("CN=intermediate", intermediateCert.getSubjectDN().toString());
        assertEquals("CN=root", intermediateCert.getIssuerDN().toString());
        assertEquals(0, intermediateCert.getBasicConstraints(), "Expected path length of 0");
    }

    private void assertOwnerReferences(OwnerReference ownerReference, String secretName) {
        if (ownerReference != null) {
            List<OwnerReference> ownerReferences = secrets.withName(secretName).get().getMetadata().getOwnerReferences();
            assertEquals(List.of(ownerReference), ownerReferences);
        } else {
            assertEquals(List.of(), secrets.withName(secretName).get().getMetadata().getOwnerReferences());
        }
    }

    private X509Certificate x509FromBase64String(String src) throws CertificateException {
        return PkiUtils.x509Certificate(Base64.getDecoder().decode(src));
    }

    private Set<String> secretNames() {
        return secrets.list().getItems().stream().map(s -> s.getMetadata().getName()).collect(Collectors.toSet());
    }
}