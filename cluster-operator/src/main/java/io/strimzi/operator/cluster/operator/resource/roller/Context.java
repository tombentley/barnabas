/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

/**
 * Associates a broker with its current state
 */
class Context {
    private final int brokerId;

    private State state = State.UNKNOWN;

    public Context(int brokerId) {
        this.brokerId = brokerId;
    }

    void validateTransition(State newState) {
        // TODO reject invalid transitions
        state = newState;
    }

    public State state() {
        return state;
    }

    public int brokerId() {
        return brokerId;
    }

    /**
     * Map an {@link Observation} of this broker to it's {@link #state()}
     */
    void classify(Observation observation) {
        if (observation.podIsReady()
                && observation.brokerStateRunning()
                && observation.isUpToDateWrtSpec()
                && observation.isMemberOfCluster()) {
            validateTransition(observation.isPreferredLeader() ? State.STABLE : State.SYNCING);
        } else if (observation.podIsReady()
                && observation.brokerStateRunning()
                && !observation.isUpToDateWrtSpec()) {
            if (observation.specIsReconfigurable()) {
                validateTransition(State.STALE_RECONFIGGABLE);
            } else if (observation.isRestartable()) {
                validateTransition(State.STALE_RESTARTABLE);
            } else {
                validateTransition(State.STALE_BLOCKED);
            }
        } else if (observation.podIsReady()
                && observation.brokerStateRecovery()) {
            validateTransition(State.RECOVERY);
        } else if (!observation.podIsReady()) {
            validateTransition(State.RESTARTED);
        } else {
            validateTransition(State.UNHEALTHY);
        }
    }

    @Override
    public String toString() {
        return "Context(" +
                "brokerId=" + brokerId +
                ", state=" + state +
                ')';
    }
}
