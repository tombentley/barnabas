/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class StUtils {

    private static final Logger LOGGER = LogManager.getLogger(StUtils.class);

    private StUtils() { }

    /**
     * Returns a map of resource name to resource version for all the pods in the given {@code namespace}
     * matching the given {@code selector}.
     */
    private static Map<String, String> podSnapshot(KubernetesClient client, String namespace, LabelSelector selector) {
        List<Pod> pods = client.pods().inNamespace(namespace).withLabels(selector.getMatchLabels()).list().getItems();
        return pods.stream()
                .collect(
                        Collectors.toMap(pod -> pod.getMetadata().getName(),
                            pod -> pod.getMetadata().getResourceVersion()));
    }

    /** Returns a map of pod name to resource version for the pods currently in the given statefulset */
    public static Map<String, String> ssSnapshot(KubernetesClient client, String namespace, String name) {
        StatefulSet statefulSet = client.apps().statefulSets().inNamespace(namespace).withName(name).get();
        LabelSelector selector = statefulSet.getSpec().getSelector();
        return podSnapshot(client, namespace, selector);
    }

    /** Returns a map of pod name to resource version for the pods currently in the given deployment */
    public static Map<String, String> depSnapshot(KubernetesClient client, String namespace, String name) {
        Deployment deployment = client.extensions().deployments().inNamespace(namespace).withName(name).get();
        LabelSelector selector = deployment.getSpec().getSelector();
        return podSnapshot(client, namespace, selector);
    }

    private static boolean ssHasRolled(KubernetesClient client, String namespace, String name, Map<String, String> snapshot) {
        if (false) {
            LOGGER.info("Existing snapshot: {}", new TreeMap(snapshot));
        }
        Map<String, String> map = podSnapshot(client, namespace, client.apps().statefulSets().inNamespace(namespace).withName(name).get().getSpec().getSelector());
        if (false) {
            LOGGER.info("Current  snapshot: {}", new TreeMap(map));
        }
        // rolled when all the pods in snapshot have a different version in map
        map.keySet().retainAll(snapshot.keySet());
        if (false) {
            LOGGER.info("Pods  in   common: {}", new TreeMap(map));
        }
        for (Map.Entry<String, String> e : map.entrySet()) {
            String currentResourceVersion = e.getValue();
            String resourceName = e.getKey();
            String oldResourceVersion = snapshot.get(resourceName);
            if (oldResourceVersion.equals(currentResourceVersion)) {
                if (false) {
                    LOGGER.info("At least {} hasn't rolled", resourceName);
                }
                return false;
            }
        }
        if (false) {
            LOGGER.info("All pods seem to have rolled");
        }
        return true;
    }

    private static boolean depHasRolled(KubernetesClient client, String namespace, String name, Map<String, String> snapshot) {
        LOGGER.info("Existing snapshot: {}", new TreeMap(snapshot));
        Map<String, String> map = podSnapshot(client, namespace, client.extensions().deployments().inNamespace(namespace).withName(name).get().getSpec().getSelector());
        LOGGER.info("Current  snapshot: {}", new TreeMap(map));
        int current = map.size();
        map.keySet().retainAll(snapshot.keySet());
        if (current == snapshot.size() && map.isEmpty()) {
            LOGGER.info("All pods seem to have rolled");
            return true;
        } else {
            LOGGER.info("Some pods still to roll: {}", map);
            return false;
        }
    }


    public static Map<String, String> waitTillSsHasRolled(KubernetesClient client, String namespace, String name, Map<String, String> snapshot) {
        TestUtils.waitFor("SS roll of " + name,
            1_000, 450_000, () -> {
                try {
                    return ssHasRolled(client, namespace, name, snapshot);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
        StUtils.waitForAllStatefulSetPodsReady(client, namespace, name);
        return ssSnapshot(client, namespace, name);
    }

    public static Map<String, String> waitTillDepHasRolled(KubernetesClient client, String namespace, String name, Map<String, String> snapshot) {
        long timeLeft = TestUtils.waitFor("Deployment roll of " + name,
            1_000, 300_000, () -> depHasRolled(client, namespace, name, snapshot));
        StUtils.waitForDeploymentReady(client, namespace, name);
        StUtils.waitForPodsReady(client, namespace, client.extensions().deployments().inNamespace(namespace).withName(name).get().getSpec().getSelector());
        return depSnapshot(client, namespace, name);
    }

    public static File downloadAndUnzip(String url) throws IOException {
        InputStream bais = (InputStream) URI.create(url).toURL().getContent();
        File dir = Files.createTempDirectory(StUtils.class.getName()).toFile();
        dir.deleteOnExit();
        ZipInputStream zin = new ZipInputStream(bais);
        ZipEntry entry = zin.getNextEntry();
        byte[] buffer = new byte[8 * 1024];
        int len;
        while (entry != null) {
            File file = new File(dir, entry.getName());
            if (entry.isDirectory()) {
                file.mkdirs();
            } else {
                FileOutputStream fout = new FileOutputStream(file);
                while ((len = zin.read(buffer)) != -1) {
                    fout.write(buffer, 0, len);
                }
                fout.close();
            }
            entry = zin.getNextEntry();
        }
        return dir;
    }

    /**
     * Wait until the SS is ready and all of its Pods are also ready
     */
    public static void waitForAllStatefulSetPodsReady(KubernetesClient client, String namespace, String name) {
        LOGGER.info("Waiting for StatefulSet {}", name);
        TestUtils.waitFor("statefulset " + name, 1_000, 300_000,
            () -> client.apps().statefulSets().inNamespace(namespace).withName(name).isReady());
        LOGGER.info("StatefulSet {} is ready", name);
        waitForPodsReady(client, name, client.apps().statefulSets().inNamespace(namespace).withName(name).get().getSpec().getSelector());
    }

    public static void waitForPodsReady(KubernetesClient client, String namespace, LabelSelector selector) {
        TestUtils.waitFor("", 1_000, 300_000, () -> {
            List<Pod> pods = client.pods().inNamespace(namespace).withLabels(selector.getMatchLabels()).list().getItems();
            for (Pod pod : pods) {
                if (!Readiness.isPodReady(pod)) {
                    return false;
                }
            }
            LOGGER.info("Pods {} are ready",
                    pods.stream().map(p -> p.getMetadata().getName()).collect(Collectors.joining(", ")));
            return true;
        });
    }

    /**
     * Wait until the deployment is ready
     */
    public static void waitForDeploymentReady(KubernetesClient client, String namespace, String name) {
        LOGGER.info("Waiting for Deployment {}", name);
        TestUtils.waitFor("deployment " + name, 1000, 300000,
            () -> client.extensions().deployments().inNamespace(namespace).withName(name).isReady());
        LOGGER.info("Deployment {} is ready", name);
    }
}
