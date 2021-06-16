/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.operator.cluster.model.pki.SubjectIdentity;

public class BrokerIdentity extends SubjectIdentity {
    private final int brokerId;

    @JsonCreator
    public BrokerIdentity(
            @JsonProperty("brokerId") int brokerId) {
        this.brokerId = brokerId;
    }

    @JsonProperty
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
