/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.Ca.CA_CRT;
import static io.strimzi.operator.cluster.model.Ca.CA_KEY;
import static io.strimzi.operator.cluster.model.Ca.CA_STORE;
import static io.strimzi.operator.cluster.model.Ca.CA_STORE_PASSWORD;
import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class CertificateRenewalTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private String clusterCaStorePassword = "123456";
    private Vertx vertx = Vertx.vertx();
    private OpenSslCertManager certManager = new OpenSslCertManager();
    private List<Secret> secrets = new ArrayList();

    @Before
    public void clearSecrets() {
        secrets = new ArrayList();
    }

    private ArgumentCaptor<Secret> reconcileCa(TestContext context, CertificateAuthority clusterCa, CertificateAuthority clientsCa) {
        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(clusterCa)
                    .withClientsCa(clientsCa)
                .endSpec()
                .build();

        return reconcileCa(context, kafka, () -> new Date());
    }

    private ArgumentCaptor<Secret> reconcileCa(TestContext context, Kafka kafka, Supplier<Date> dateSupplier) {
        SecretOperator secretOps = mock(SecretOperator.class);

        when(secretOps.list(eq(NAMESPACE), any())).thenAnswer(invocation -> {
            Map<String, String> requiredLabels = ((Labels) invocation.getArgument(1)).toMap();
            return secrets.stream().filter(s -> {
                Map<String, String> labels = new HashMap(s.getMetadata().getLabels());
                labels.keySet().retainAll(requiredLabels.keySet());
                return labels.equals(requiredLabels);
            }).collect(Collectors.toList());
        });
        ArgumentCaptor<Secret> c = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaCertSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9), certManager,
                new ResourceOperatorSupplier(null, null, null,
                        null, null, secretOps, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null, null),
                ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        AtomicReference<Throwable> error = new AtomicReference<>();
        Async async = context.async();
        op.new ReconciliationState(reconciliation, kafka).reconcileCas(dateSupplier).setHandler(ar -> {
            error.set(ar.cause());
            async.complete();
        });
        async.await();
        if (error.get() != null) {
            Throwable t = error.get();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t);
            }
        }
        return c;
    }

    private CertAndKey generateCa(OpenSslCertManager certManager, CertificateAuthority certificateAuthority, String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        File clusterCaKeyFile = File.createTempFile("tls", "cluster-ca-key");
        File clusterCaCertFile = File.createTempFile("tls", "cluster-ca-cert");
        File clusterCaStoreFile = File.createTempFile("tls", "cluster-ca-store");
        try {
            Subject sbj = new Subject();
            sbj.setOrganizationName("io.strimzi");
            sbj.setCommonName(commonName);

            certManager.generateSelfSignedCert(clusterCaKeyFile, clusterCaCertFile, sbj, ModelUtils.getCertificateValidity(certificateAuthority));
            certManager.addCertToTrustStore(clusterCaCertFile, CA_CRT, clusterCaStoreFile, clusterCaStorePassword);
            return new CertAndKey(
                    Files.readAllBytes(clusterCaKeyFile.toPath()),
                    Files.readAllBytes(clusterCaCertFile.toPath()),
                    Files.readAllBytes(clusterCaStoreFile.toPath()),
                    null,
                    clusterCaStorePassword);
        } finally {
            clusterCaKeyFile.delete();
            clusterCaCertFile.delete();
            clusterCaStoreFile.delete();
        }
    }

    private List<Secret> initialClusterCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "cluster-ca",
                AbstractModel.clusterCaKeySecretName(NAME),
                AbstractModel.clusterCaCertSecretName(NAME));
    }

    private List<Secret> initialClientsCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "clients-ca",
                KafkaCluster.clientsCaKeySecretName(NAME),
                KafkaCluster.clientsCaCertSecretName(NAME));
    }

    private List<Secret> initialCaSecrets(CertificateAuthority certificateAuthority, String commonName,
                                          String caKeySecretName, String caCertSecretName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        List<Secret> secrets = new ArrayList<>();
        secrets.add(
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, caKeySecretName, result.keyAsBase64String())
        );
        secrets.add(
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, caCertSecretName,
                        result.certAsBase64String(), result.trustStoreAsBase64String(), result.storePasswordAsBase64String())
        );
        return secrets;
    }

    private boolean isCertInTrustStore(String alias, Map<String, String> data)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Base64.getDecoder().decode(data.get(CA_STORE))),
                new String(Base64.getDecoder().decode(data.get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII).toCharArray()
        );
        return trustStore.isCertificateEntry(alias);
    }

    private X509Certificate getCertificate(String alias, Map<String, String> data)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Base64.getDecoder().decode(data.get(CA_STORE))),
                new String(Base64.getDecoder().decode(data.get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII).toCharArray()
        );
        return (X509Certificate) trustStore.getCertificate(alias);
    }

    @Test
    public void certsGetGeneratedInitiallyAuto(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        secrets.clear();
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), c.getAllValues().get(0).getData().keySet());
        assertTrue(isCertInTrustStore(CA_CRT, c.getAllValues().get(0).getData()));
        assertEquals(singleton(CA_KEY), c.getAllValues().get(1).getData().keySet());
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), c.getAllValues().get(2).getData().keySet());
        assertTrue(isCertInTrustStore(CA_CRT, c.getAllValues().get(2).getData()));
        assertEquals(singleton(CA_KEY), c.getAllValues().get(3).getData().keySet());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void failsWhenCustomCertsAreMissing(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();
        secrets.clear();
        reconcileCa(context, certificateAuthority, certificateAuthority);
    }

    @Test
    public void noCertsGetGeneratedOutsideRenewalPeriodAuto(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        noCertsGetGeneratedOutsideRenewalPeriod(context, true);
    }

    private void noCertsGetGeneratedOutsideRenewalPeriod(TestContext context, boolean generateCertificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(generateCertificateAuthority)
                .build();
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);

        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), c.getAllValues().get(0).getData().keySet());
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), c.getAllValues().get(0).getData().get(CA_CRT));
        assertTrue(x509Certificate(initialClusterCaCertSecret.getData().get(CA_CRT))
                .equals(getCertificate(CA_CRT, c.getAllValues().get(0).getData())));

        assertEquals(set(CA_KEY), c.getAllValues().get(1).getData().keySet());
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), c.getAllValues().get(1).getData().get(CA_KEY));

        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), c.getAllValues().get(2).getData().keySet());
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), c.getAllValues().get(2).getData().get(CA_CRT));
        assertTrue(x509Certificate(initialClientsCaCertSecret.getData().get(CA_CRT))
                .equals(getCertificate(CA_CRT, c.getAllValues().get(2).getData())));

        assertEquals(set(CA_KEY), c.getAllValues().get(3).getData().keySet());
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), c.getAllValues().get(3).getData().get(CA_KEY));
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAuto(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clusterCaCertData.keySet());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClusterCaCert);
        assertNotNull(newClusterCaCertStore);
        assertNotNull(newClusterCaCertStorePassword);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clientsCaCertData.keySet());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClientsCaCert);
        assertNotNull(newClientsCaCertStore);
        assertNotNull(newClientsCaCertStorePassword);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAutoOutsideOfMaintenanceWindow(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clusterCaCertData.keySet());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        assertEquals("0", c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClusterCaCert);
        assertNotNull(newClusterCaCertStore);
        assertNotNull(newClusterCaCertStorePassword);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        assertEquals("0", c.getAllValues().get(1).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clientsCaCertData.keySet());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        assertEquals("0", c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClientsCaCert);
        assertNotNull(newClientsCaCertStore);
        assertNotNull(newClientsCaCertStorePassword);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        assertEquals("0", c.getAllValues().get(3).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAutoWithinMaintenanceWindow(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 12, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clusterCaCertData.keySet());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        assertEquals("1", c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClusterCaCert);
        assertNotNull(newClusterCaCertStore);
        assertNotNull(newClusterCaCertStorePassword);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        assertEquals("0", c.getAllValues().get(1).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), clientsCaCertData.keySet());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        assertEquals("1", c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertNotNull(newClientsCaCert);
        assertNotNull(newClientsCaCertStore);
        assertNotNull(newClientsCaCertStorePassword);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        assertEquals("0", c.getAllValues().get(3).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAuto(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(4, clusterCaCertData.size());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));
        Map.Entry oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), oldClusterCaCert.getValue());
        assertEquals("CN=cluster-ca v1, O=io.strimzi", x509Certificate(newClusterCaCert).getSubjectDN().getName());

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertEquals("1", clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertNotEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(4, clientsCaCertData.size());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));
        Map.Entry oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), oldClientsCaCert.getValue());
        assertEquals("CN=clients-ca v1, O=io.strimzi", x509Certificate(newClientsCaCert).getSubjectDN().getName());

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertEquals("1", clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertNotEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAutoOutsideOfTimeWindow(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 0, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals("0", c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        assertEquals(3, clusterCaCertData.size());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));
        assertEquals("CN=cluster-ca, O=io.strimzi", x509Certificate(newClusterCaCert).getSubjectDN().getName());

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertEquals("0", clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals("0", c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        assertEquals(3, clientsCaCertData.size());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));
        assertEquals("CN=clients-ca, O=io.strimzi", x509Certificate(newClientsCaCert).getSubjectDN().getName());

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertEquals("0", clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAutoWithinTimeWindow(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 12, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals("1", c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        assertEquals(4, clusterCaCertData.size());
        X509Certificate newX509ClusterCaCertStore = getCertificate(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_STORE), newClusterCaCertStore);
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), newClusterCaCertStorePassword);
        assertTrue(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)));
        Map.Entry oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), oldClusterCaCert.getValue());
        assertEquals("CN=cluster-ca v1, O=io.strimzi", x509Certificate(newClusterCaCert).getSubjectDN().getName());

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertEquals("1", clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertNotEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals("1", c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION));
        assertEquals(4, clientsCaCertData.size());
        X509Certificate newX509ClientsCaCertStore = getCertificate(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_STORE), newClientsCaCertStore);
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), newClientsCaCertStorePassword);
        assertTrue(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)));
        Map.Entry oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), oldClientsCaCert.getValue());
        assertEquals("CN=clients-ca v1, O=io.strimzi", x509Certificate(newClientsCaCert).getSubjectDN().getName());

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertEquals("1", clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertNotEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(newClusterCaCert)));
    }

    @Test
    public void expiredCertsGetRemovedAuto(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));

        // add an expired certificate to the secret ...
        initialClusterCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                        TestUtils.readResource(getClass(), "cluster-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));
        // ... and to the related truststore
        File certFile = File.createTempFile("tls", "-cert");
        Files.write(certFile.toPath(), Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        File trustStoreFile = File.createTempFile("tls", "-truststore");
        Files.write(trustStoreFile.toPath(), Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get(CA_STORE)));
        String trustStorePassword = new String(Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII);
        certManager.addCertToTrustStore(certFile, "ca-2018-07-01T09-00-00.crt", trustStoreFile, trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
        assertTrue(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClusterCaCertSecret.getData()));
        trustStoreFile.delete();
        certFile.delete();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));

        // add an expired certificate to the secret ...
        initialClientsCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                TestUtils.readResource(getClass(), "clients-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));
        // ... and to the related truststore
        certFile = File.createTempFile("tls", "-cert");
        Files.write(certFile.toPath(), Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        trustStoreFile = File.createTempFile("tls", "-truststore");
        Files.write(trustStoreFile.toPath(), Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get(CA_STORE)));
        trustStorePassword = new String(Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII);
        certManager.addCertToTrustStore(certFile, "ca-2018-07-01T09-00-00.crt", trustStoreFile, trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
        assertTrue(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClientsCaCertSecret.getData()));
        trustStoreFile.delete();
        certFile.delete();

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(clusterCaCertData.keySet().toString(), 3, clusterCaCertData.size());
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), clusterCaCertData.get(CA_CRT));
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE), clusterCaCertData.get(CA_STORE));
        assertEquals(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), clusterCaCertData.get(CA_STORE_PASSWORD));
        assertTrue(getCertificate(CA_CRT, clusterCaCertData).equals(x509Certificate(clusterCaCertData.get(CA_CRT))));
        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), clusterCaKeyData.get(CA_KEY));
        assertFalse(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clusterCaCertData));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(clientsCaCertData.keySet().toString(), 3, clientsCaCertData.size());
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), clientsCaCertData.get(CA_CRT));
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE), clientsCaCertData.get(CA_STORE));
        assertEquals(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), clientsCaCertData.get(CA_STORE_PASSWORD));
        assertTrue(getCertificate(CA_CRT, clientsCaCertData).equals(x509Certificate(clientsCaCertData.get(CA_CRT))));
        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), clientsCaKeyData.get(CA_KEY));
        assertFalse(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clientsCaCertData));
    }

    @Test
    public void customCertsNotReconciled(TestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertEquals(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE));
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        assertTrue(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(0, c.getAllValues().size());
    }
}