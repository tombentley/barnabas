/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

class ProcessorImpl implements Processor {

    public void reconfigure(int broker) {
        throw new RuntimeException("Not yet implemented");
    }

    public void awaitStable(int broker) {
        throw new RuntimeException("Not yet implemented");
    }

    public void deletePod(int broker) {
        throw new RuntimeException("Not yet implemented");
    }
}
