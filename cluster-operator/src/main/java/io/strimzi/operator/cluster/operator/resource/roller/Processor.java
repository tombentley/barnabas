/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

interface Processor {

    void deletePod(int broker);

    void awaitStable(int broker);

    void reconfigure(int broker);
}
