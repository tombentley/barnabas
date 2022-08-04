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
    private boolean processed;

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
        validateTransition(classify2(observation));
    }
    static State classify2(Observation observation) {
        if (observation.podIsReady()
                && observation.brokerStateRunning()
                && observation.isUpToDateWrtSpec()
                && observation.isMemberOfCluster()) {
            return observation.isPreferredLeader() ? State.STABLE : State.SYNCING;
        } else if (observation.podIsReady()
                && observation.brokerStateRunning()
                && !observation.isUpToDateWrtSpec()) {
            if (observation.specIsReconfigurable()) {
                return State.STALE_RECONFIGGABLE;
            } else if (observation.isRestartable()) {
                return State.STALE_RESTARTABLE;
            } else {
                return State.STALE_BLOCKED;
            }
        } else if (observation.podIsReady()
                && observation.brokerStateRecovery()) {
            return State.RECOVERY;
        } else if (!observation.podIsReady()) {
            return State.RESTARTED;
        } else {
            return State.UNHEALTHY;
        }
    }

    @Override
    public String toString() {
        return "Context(" +
                "brokerId=" + brokerId +
                ", state=" + state +
                ')';
    }

    public void touched() {
        if (processed) {
            throw new TouchedTwiceException("Already touched " + this);
        }
        this.processed = true;
    }
}
