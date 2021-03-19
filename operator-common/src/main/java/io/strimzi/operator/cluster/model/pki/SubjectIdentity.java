/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

/**
 * Identity for a certificate. This is a different thing to the certificate's subject, or {@link io.strimzi.certs.Subject}.
 * It identifies an entity which needs a certificate, independently of the subject for that certificate.
 * This is needed because Strimzi used the same subject for all broker certificates.
 */
public abstract class SubjectIdentity {
    @Override
    public abstract boolean equals(Object other);
    @Override
    public abstract int hashCode();
    @Override
    public abstract String toString();
}
class BrokerIdentity extends SubjectIdentity{
    private final int brokerId;

    public BrokerIdentity(int brokerId) {
        this.brokerId = brokerId;
    }

    public int brokerId() {
        return brokerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerIdentity that = (BrokerIdentity) o;
        return brokerId == that.brokerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId);
    }

    @Override
    public String toString() {
        return "BrokerIdentity(" +
                "brokerId=" + brokerId +
                ')';
    }
}

class ZooKeeperNodeIdentity extends SubjectIdentity{
    private final int nodeId;

    public ZooKeeperNodeIdentity(int nodeId) {
        this.nodeId = nodeId;
    }

    public int brokerId() {
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
