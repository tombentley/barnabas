/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.client;


import io.fabric8.openshift.client.OpenShiftClient;

public interface StrimziOpenShiftClient extends OpenShiftClient {
    StrimziOpenShiftAPIGroupDSL strimzi();
}
