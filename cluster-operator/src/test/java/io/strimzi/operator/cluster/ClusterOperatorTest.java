/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import okhttp3.OkHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class ClusterOperatorTest {

    private Vertx vertx;

    private static Map<String, String> buildEnv(String namespaces) {
        Map<String, String> env = new HashMap<>();
        env.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, namespaces);
        env.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "120000");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo");
        return env;
    }

    @Before
    public void createClient(TestContext context) {
        vertx = Vertx.vertx();
    }

    @After
    public void closeClient() {
        vertx.close();
    }

    @Test
    public void startStopSingleNamespaceOs(TestContext context) {
        startStop(context, "namespace", true);
    }

    @Test
    public void startStopMultiNamespaceOs(TestContext context) {
        startStop(context, "namespace1,namespace2", true);
    }

    @Test
    public void startStopSingleNamespaceK8s(TestContext context) {
        startStop(context, "namespace", false);
    }

    @Test
    public void startStopMultiNamespaceK8s(TestContext context) {
        startStop(context, "namespace1,namespace2", false);
    }

    @Test
    public void startStopAllNamespacesOs(TestContext context) {
        startStopAllNamespaces(context, "*", true);
    }

    @Test
    public void startStopAllNamespacesK8s(TestContext context) {
        startStopAllNamespaces(context, "*", false);
    }

    /**
     * Does the CO start and then stop a verticle per namespace?
     * @param context
     * @param namespaces
     */
    private void startStop(TestContext context, String namespaces, boolean openShift) {
        AtomicInteger numWatchers = new AtomicInteger(0);
        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }
        when(client.isAdaptable(eq(OkHttpClient.class))).thenReturn(true);
        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        MixedOperation mockCms = mock(MixedOperation.class);
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition, Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>> mockCrds = mock(NonNamespaceOperation.class);
        Resource<CustomResourceDefinition, DoneableCustomResourceDefinition> mockResource = mock(Resource.class);
        if (openShift) {
            when(mockResource.get()).thenReturn(Crds.kafkaConnectS2I());
        } else {
            when(mockResource.get()).thenReturn(null);
        }
        when(mockCrds.withName(KafkaConnectS2I.CRD_NAME)).thenReturn(mockResource);
        when(client.customResourceDefinitions()).thenReturn(mockCrds);
        when(client.customResources(any(), any(), any(), any())).thenReturn(mockCms);

        List<String> namespaceList = asList(namespaces.split(" *,+ *"));
        for (String namespace: namespaceList) {

            MixedOperation mockNamespacedCms = mock(MixedOperation.class);
            when(mockNamespacedCms.watch(any())).thenAnswer(invo -> {
                numWatchers.incrementAndGet();
                Watch mockWatch = mock(Watch.class);
                doAnswer(invo2 -> {
                    ((Watcher) invo.getArgument(0)).onClose(null);
                    return null;
                }).when(mockWatch).close();
                return mockWatch;
            });

            when(mockNamespacedCms.withLabels(any())).thenReturn(mockNamespacedCms);
            when(mockCms.inNamespace(namespace)).thenReturn(mockNamespacedCms);
        }

        Async async = context.async();

        Map<String, String> env = buildEnv(namespaces);
        Main.run(vertx, client, new PlatformFeaturesAvailability(openShift, KubernetesVersion.V1_9), ClusterOperatorConfig.fromMap(env)).setHandler(ar -> {
            context.assertNull(ar.cause(), "Expected all verticles to start OK");
            async.complete();
        });
        async.await();

        context.assertEquals(namespaceList.size(), vertx.deploymentIDs().size(), "A verticle per namespace");

        List<Async> asyncs = new ArrayList<>();
        for (String deploymentId: vertx.deploymentIDs()) {
            Async async2 = context.async();
            asyncs.add(async2);
            vertx.undeploy(deploymentId, ar -> {
                context.assertNull(ar.cause(), "Didn't expect error when undeploying verticle " + deploymentId);
                async2.complete();
            });
        }

        for (Async async2: asyncs) {
            async2.await();
        }

        if (numWatchers.get() > (openShift ? 7 : 5) * namespaceList.size()) { // we do not have connectS2I on k8s
            context.fail("Looks like there were more watchers than namespaces");
        }
    }

    /**
     * Does the CO start and then stop with the namespace wildcard (*)?
     * @param context
     * @param namespaces
     */
    private void startStopAllNamespaces(TestContext context, String namespaces, boolean openShift) {
        AtomicInteger numWatchers = new AtomicInteger(0);
        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }
        when(client.isAdaptable(eq(OkHttpClient.class))).thenReturn(true);
        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        MixedOperation mockCms = mock(MixedOperation.class);
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition, Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>> mockCrds = mock(NonNamespaceOperation.class);
        Resource<CustomResourceDefinition, DoneableCustomResourceDefinition> mockResource = mock(Resource.class);
        if (openShift) {
            when(mockResource.get()).thenReturn(Crds.kafkaConnectS2I());
        } else {
            when(mockResource.get()).thenReturn(null);
        }
        when(mockCrds.withName(KafkaConnectS2I.CRD_NAME)).thenReturn(mockResource);
        when(client.customResourceDefinitions()).thenReturn(mockCrds);
        when(client.customResources(any(), any(), any(), any())).thenReturn(mockCms);

        FilterWatchListMultiDeletable mockFilteredCms = mock(FilterWatchListMultiDeletable.class);
        when(mockFilteredCms.withLabels(any())).thenReturn(mockFilteredCms);
        when(mockFilteredCms.watch(any())).thenAnswer(invo -> {
            numWatchers.incrementAndGet();
            Watch mockWatch = mock(Watch.class);
            doAnswer(invo2 -> {
                ((Watcher) invo.getArgument(0)).onClose(null);
                return null;
            }).when(mockWatch).close();
            return mockWatch;
        });
        when(mockCms.inAnyNamespace()).thenReturn(mockFilteredCms);

        Async async = context.async();

        Map<String, String> env = buildEnv(namespaces);
        Main.run(vertx, client, new PlatformFeaturesAvailability(openShift, KubernetesVersion.V1_9), ClusterOperatorConfig.fromMap(env)).setHandler(ar -> {
            context.assertNull(ar.cause(), "Expected all verticles to start OK");
            async.complete();
        });
        async.await();

        context.assertEquals(1, vertx.deploymentIDs().size(), "A verticle per namespace");

        List<Async> asyncs = new ArrayList<>();
        for (String deploymentId: vertx.deploymentIDs()) {
            Async async2 = context.async();
            asyncs.add(async2);
            vertx.undeploy(deploymentId, ar -> {
                context.assertNull(ar.cause(), "Didn't expect error when undeploying verticle " + deploymentId);
                async2.complete();
            });
        }

        for (Async async2: asyncs) {
            async2.await();
        }

        if (numWatchers.get() > (openShift ? 7 : 5)) { // we do not have connectS2I on k8s
            context.fail("Looks like there were more watchers than should be");
        }
    }
}
