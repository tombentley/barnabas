/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

public class Zookeeper extends AbstractSsLike {
    public Zookeeper() {
        this.image = "strimzi/kafka:latest";
    }
}
