/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import net.jqwik.api.stateful.Action;

class ObservationAction implements Action<KR2> {

    private final int brokerId;
    private final Observation newObservation;

    public ObservationAction(int brokerId, Observation newObservation) {
        this.brokerId = brokerId;
        this.newObservation = newObservation;
    }

    @Override
    public KR2 run(KR2 roller) {
        ((MockObserver) roller.observer).observations.put(brokerId, newObservation);
        roller.processOne();
        // TODO add assertions
        return roller;
    }

    @Override
    public String toString() {
        return "ObservationAction(" +
                "brokerId=" + brokerId +
                ", newState=" + Context.classify2(newObservation) +
                " implied by newObservation=" + newObservation +
                ')';
    }
}
