/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.function.Supplier;

import io.strimzi.operator.common.BackOff;
import io.vertx.core.Promise;

class RestartContext {
    private final Promise<Void> promise;
    private final BackOff backOff;
    private BrokerState state;
    private long connectionErrorStart = 0L;
    ////
    private boolean needsRestart;
    private boolean needsReconfig;
    private boolean forceRestart;
    private KafkaBrokerConfigurationDiff diff;
    private KafkaBrokerLoggingConfigurationDiff logDiff;

    RestartContext(Supplier<BackOff> backOffSupplier) {
        this.setState(BrokerState.UNKNOWN);
        promise = Promise.promise();
        backOff = backOffSupplier.get();
        getBackOff().delayMs();
    }

    public void clearConnectionError() {
        setConnectionErrorStart(0L);
    }

    long connectionError() {
        return getConnectionErrorStart();
    }

    void noteConnectionError() {
        if (getConnectionErrorStart() == 0L) {
            setConnectionErrorStart(System.currentTimeMillis());
        }
    }

    @Override
    public String toString() {
        return "RestartContext{" +
                "promise=" + getPromise() +
                ", backOff=" + getBackOff() +
                '}';
    }

    public Promise<Void> getPromise() {
        return promise;
    }

    public BackOff getBackOff() {
        return backOff;
    }

    public BrokerState getState() {
        return state;
    }

    public void setState(BrokerState state) {
        this.state = state;
    }

    public long getConnectionErrorStart() {
        return connectionErrorStart;
    }

    public void setConnectionErrorStart(long connectionErrorStart) {
        this.connectionErrorStart = connectionErrorStart;
    }

    public boolean isNeedsRestart() {
        return needsRestart;
    }

    public void setNeedsRestart(boolean needsRestart) {
        this.needsRestart = needsRestart;
    }

    public boolean isNeedsReconfig() {
        return needsReconfig;
    }

    public void setNeedsReconfig(boolean needsReconfig) {
        this.needsReconfig = needsReconfig;
    }

    public boolean isForceRestart() {
        return forceRestart;
    }

    public void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }

    public KafkaBrokerConfigurationDiff getDiff() {
        return diff;
    }

    public void setDiff(KafkaBrokerConfigurationDiff diff) {
        this.diff = diff;
    }

    public KafkaBrokerLoggingConfigurationDiff getLogDiff() {
        return logDiff;
    }

    public void setLogDiff(KafkaBrokerLoggingConfigurationDiff logDiff) {
        this.logDiff = logDiff;
    }

    enum BrokerState {
        UNKNOWN,
        UNSCHEDULABLE,
        AWAITING_LEADERSHIP,
        AWAITING_READY,
        NEEDS_RESTART,
        NEEDS_RECONFIG,
        UNREADY,
        NEEDS_CLASSIFY,
        READY,
        CONTROLLER
    }
}
