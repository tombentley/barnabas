/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

class ProcessorImpl implements Processor {
    @Override
    public void process(Context context) {
        switch (context.state()) {

            case STALE_RESTARTABLE:
            case UNHEALTHY:
                deletePod();
                awaitStable();
                break;
            case STALE_RECONFIGGABLE:
                reconfigure();
                awaitStable();
                break;
            default:
                throw new IllegalStateException("Unexpected state " + context);
        }
    }

    private void reconfigure() {
        throw new RuntimeException("Not yet implemented");
    }

    private void awaitStable() {
        throw new RuntimeException("Not yet implemented");
    }

    private void deletePod() {
        throw new RuntimeException("Not yet implemented");
    }
}
