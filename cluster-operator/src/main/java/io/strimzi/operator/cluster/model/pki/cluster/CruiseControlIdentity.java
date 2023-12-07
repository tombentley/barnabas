/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.strimzi.operator.cluster.model.pki.SubjectIdentity;

public final class CruiseControlIdentity extends SubjectIdentity {

    @JsonCreator
    public CruiseControlIdentity() {
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CruiseControlIdentity;
    }

    @Override
    public int hashCode() {
        return 5689;
    }

    @Override
    public String toString() {
        return "CruiseControlIdentity";
    }
}
