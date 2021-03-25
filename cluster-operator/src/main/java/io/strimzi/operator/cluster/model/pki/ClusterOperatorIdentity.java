/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public class ClusterOperatorIdentity extends SubjectIdentity {

    public static final ClusterOperatorIdentity INSTANCE = new ClusterOperatorIdentity();

    private ClusterOperatorIdentity() {
    }

    @Override
    public boolean equals(Object other) {
        return other == INSTANCE;
    }

    @Override
    public int hashCode() {
        return 1234;
    }

    @Override
    public String toString() {
        return "ClusterOperatorIdentity";
    }
}
