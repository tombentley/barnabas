/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.util.Objects;

import io.strimzi.operator.cluster.model.pki.SubjectIdentity;

public class ZooKeeperNodeIdentity extends SubjectIdentity {
    private final int nodeId;

    public ZooKeeperNodeIdentity(int nodeId) {
        if (nodeId <= 0)
            throw new IllegalArgumentException();
        this.nodeId = nodeId;
    }

    public int nodeId() {
        return nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperNodeIdentity that = (ZooKeeperNodeIdentity) o;
        return nodeId == that.nodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "ZooKeeperNodeIdentity(" +
                "nodeId=" + nodeId +
                ')';
    }
}
