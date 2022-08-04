/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import java.util.List;
import java.util.Map;

interface Observer {
    public Map<Integer, Observation> observe(List<Context> contexts);
}
