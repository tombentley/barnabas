/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import io.fabric8.kubernetes.api.model.Pod;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.Node;

/**
 * Observations about a broker.
 * These observations are mapped to a {@link State} by {@link Context#classify(Observation)}.
 */
class Observation {
    static Observation create(Pod pod,
                              Node node,
                              Config config,
                              Config loggerConfig) {

        var podIsReady = pod.getStatus().getConditions().stream()
                .anyMatch(c -> "Ready".equals(c.getType()) && "true".equals(c.getStatus()));
        var brokerStateRunning = false; // TODO consume broker state
        var isUpToDateWithSpec = false; // TODO compare pod annotation with sts/sps generation
        var isMemberOfCluster = node != null;
        var isPreferredLeader = false; // TODO
        var specIsReconfigurable = false; // TODO
        var brokerStateRecovery = false;
        var isRestartable = false;
        return new Observation(podIsReady,
                brokerStateRunning,
                isUpToDateWithSpec,
                isMemberOfCluster,
                isPreferredLeader,
                specIsReconfigurable,
                brokerStateRecovery,
                isRestartable);
    }

    private final boolean podIsReady;
    private final boolean brokerStateRunning;
    private final boolean isUpToDateWrtSpec;
    private final boolean isMemberOfCluster;
    private final boolean isPreferredLeader;
    private final boolean specIsReconfigurable;
    private final boolean brokerStateRecovery;

    private final boolean isRestartable;

    public Observation(boolean podIsReady,
                       boolean brokerStateRunning,
                       boolean isUpToDateWrtSpec,
                       boolean isMemberOfCluster,
                       boolean isPreferredLeader,
                       boolean specIsReconfigurable,
                       boolean brokerStateRecovery,
                       boolean isRestartable) {
        this.podIsReady = podIsReady;
        this.brokerStateRunning = brokerStateRunning;
        this.isUpToDateWrtSpec = isUpToDateWrtSpec;
        this.isMemberOfCluster = isMemberOfCluster;
        this.isPreferredLeader = isPreferredLeader;
        this.specIsReconfigurable = specIsReconfigurable;
        this.brokerStateRecovery = brokerStateRecovery;
        this.isRestartable = isRestartable;
    }

    public boolean podIsReady() {
        return podIsReady;
    }

    public boolean brokerStateRunning() {
        return brokerStateRunning;
    }

    public boolean isUpToDateWrtSpec() {
        return isUpToDateWrtSpec;
    }

    public boolean isMemberOfCluster() {
        return isMemberOfCluster;
    }

    public boolean isPreferredLeader() {
        return isPreferredLeader;
    }

    public boolean specIsReconfigurable() {
        return specIsReconfigurable;
    }

    public boolean brokerStateRecovery() {
        return brokerStateRecovery;
    }

    public boolean isRestartable() {
        return isRestartable;
    }

    @Override
    public String toString() {
        return "Observation(" +
                "podIsReady=" + podIsReady +
                ", brokerStateRunning=" + brokerStateRunning +
                ", isUpToDateWrtSpec=" + isUpToDateWrtSpec +
                ", isMemberOfCluster=" + isMemberOfCluster +
                ", isPreferredLeader=" + isPreferredLeader +
                ", specIsReconfigurable=" + specIsReconfigurable +
                ", brokerStateRecovery=" + brokerStateRecovery +
                ", isRestartable=" + isRestartable +
                ')';
    }
}
