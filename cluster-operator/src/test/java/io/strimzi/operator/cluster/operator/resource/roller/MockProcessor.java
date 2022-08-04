/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import java.util.Map;

class MockProcessor implements Processor {

    final Map<Integer, Observation> observations;

    public MockProcessor(Map<Integer, Observation> observations) {
        this.observations = observations;
    }

    @Override
    public void deletePod(int broker) {
        observations.put(broker, new Observation(
                false,
                false,
                true,
                false,
                false,
                true,
                false,
                true)); // TODO use an Abritrary here?
    }

    @Override
    public void reconfigure(int broker) {
        observations.put(broker, new Observation(
                true,
                true,
                true,
                true,
                true,
                false,
                false,
                true));
    }

    @Override
    public void awaitStable(int broker) {
        observations.put(broker, new Observation(
                true,
                true,
                true,
                true,
                true,
                true,
                false,
                true));
    }
}
