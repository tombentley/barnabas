/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaMirrorMaker} cluster.
 */
public class KafkaMirrorMakerResources {
    protected KafkaMirrorMakerResources() { }

    /**
     * Returns the name of the Kafka Mirror Maker {@code Deployment} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka Mirror Maker {@code Deployment}.
     */
    public static String deploymentName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    /**
     * Returns the name of the Kafka Mirror Maker {@code ServiceAccount} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka Mirror Maker {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the Prometheus {@code Service} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    /**
     * Returns the name of the Kafka Mirror Maker metrics and log {@code ConfigMap} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka Mirror Maker metrics and log {@code ConfigMap}.
     */
    public static String metricsAndLogConfigMapName(String clusterName) {
        return clusterName + "-mirror-maker-config";
    }
}
