/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaConnectAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = AbstractModel.getOrderedProperties("kafkaConnectDefaultLoggingProperties")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.");

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // No metrics config  => no CMs created
            Set<String> metricsNames = new HashSet<>();
            if (connect.isMetricsEnabled()) {
                metricsNames.add(KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName));
            }

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(connect.getServiceName(), service.getMetadata().getName());
            context.assertEquals(connect.generateService(), service, "Services are not equal");

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.assertEquals(connect.generateDeployment(annotations, true, null, null), dc, "Deployments are not equal");

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.assertEquals(connect.getName(), pdb.getMetadata().getName());
            context.assertEquals(connect.generatePodDisruptionBudget(), pdb, "PodDisruptionBudgets are not equal");

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.assertEquals(capturedConnects.get(0).getStatus().getUrl(), "http://foo-connect-api.test.svc:8083");
            context.assertEquals(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), "True");
            context.assertEquals(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), "Ready");
            async.complete();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(clusterCmNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());

            // Verify Deployment Config
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.assertEquals(connect.getName(), pdb.getMetadata().getName());
            context.assertEquals(connect.generatePodDisruptionBudget(), pdb, "PodDisruptionBudgets are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateCluster(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(clusterCmNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(clusterCmNamespace, KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            KafkaConnectCluster compareTo = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(compareTo.getServiceName(), service.getMetadata().getName());
            context.assertEquals(compareTo.generateService(), service, "Services are not equal");

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            Map<String, String> annotations = new HashMap();
            annotations.put("strimzi.io/logging", loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG));
            context.assertEquals(compareTo.generateDeployment(annotations, true, null, null), dc, "Deployments are not equal");

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.assertEquals(connect.getName(), pdb.getMetadata().getName());
            context.assertEquals(connect.generatePodDisruptionBudget(), pdb, "PodDisruptionBudgets are not equal");

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).createOrUpdate(any());
            async.complete();
        });
    }

    @Test
    public void testUpdateClusterFailure(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertFalse(createResult.succeeded());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleUp(TestContext context) {
        final int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleDown(TestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            if (createResult.failed()) {
                createResult.cause().printStackTrace();
            }
            context.assertTrue(createResult.succeeded());

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;

        String clusterCmNamespace = "test";

        KafkaConnect foo = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, "foo");
        KafkaConnect bar = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, "bar");
        when(mockConnectOps.listAsync(eq(clusterCmNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect cluster
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka Connect clusters
        Labels newLabels = Labels.forKind(KafkaConnect.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Connect clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Async async = context.async(2);
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnectAssembly) {
                createdOrUpdated.add(kafkaConnectAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka Connect clusters
        ops.reconcileAll("test", clusterCmNamespace, ignored -> { });

        async.await();

        context.assertEquals(new HashSet(asList("foo", "bar")), createdOrUpdated);
    }

    @Test
    public void testCreateClusterStatusNotReady(TestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        String failureMsg = "failure";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertFalse(createResult.succeeded());

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.assertEquals(capturedConnects.get(0).getStatus().getUrl(), "http://foo-connect-api.test.svc:8083");
            context.assertEquals(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), "True");
            context.assertEquals(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), "NotReady");
            context.assertEquals(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(), failureMsg);
            async.complete();
        });
    }

}
