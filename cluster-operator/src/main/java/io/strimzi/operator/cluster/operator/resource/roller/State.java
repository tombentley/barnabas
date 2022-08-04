/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

/**
 * Enumerates possible states of brokers with respect to their rollability.
 * Each state has a priority which is used by {@link KR2#roll()} to group brokers
 * and process them in the appropriate order.
 */
enum State {
    /** The initial state, where we know nothing about the broker */
    UNKNOWN(-1),
    /** The broker needs to be restarted, and (based on the last observation) can be restarted safely now */
    STALE_RESTARTABLE(2),
    /** The broker has been restarted, but is not yet running */
    RESTARTED(0),
    /**
     * The broker has been restarted, and is running, but is doing log recovery.
     * It is not yet part of the cluster.
     */
    RECOVERY(0),
    /**
     * The broker has been restarted, and completed log recovery,
     * but it is not yet the current leader for all the topic partitions for which
     * it is the preferred leader.
     * It may not be in the ISR, or if it is then it has not yet been elected leader.
     */
    SYNCING(0),
    /** The broker needs to be restarted, but (based on the last observation) cannot be restarted safely now */
    STALE_BLOCKED(4),
    /**
     * The broker is the current leader for all the topic partitions for which
     * it is the preferred leader.
     */
    STABLE(5),
    /**
     * The broker does not need to be restarted, but does need to be reconfigured and the reconfiguration
     * can be done dynamically.
     */
    STALE_RECONFIGGABLE(3),
    /**
     * The broker does not appear to be running.
     */
    UNHEALTHY(1);
    private final int priority;

    private State(int priority) {
        this.priority = priority;
    }

    public boolean isHealthy() {
        return this == STABLE || this == SYNCING;
    }

    public boolean isRestarting() {
        return this == RESTARTED || this == RECOVERY;
    }

    public boolean needsRestart() {
        return this == UNHEALTHY || this == STALE_RESTARTABLE;
    }

    public int priority() {
        return priority;
    }
}
