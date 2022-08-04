/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import java.util.List;
import java.util.Map;

class MockObserver implements Observer {

    final Map<Integer, Observation> observations;

    MockObserver(Map<Integer, Observation> observations) {
        this.observations = observations;
    }

    @Override
    public Map<Integer, Observation> observe(List<Context> contexts) {
        return observations;
    }
}
