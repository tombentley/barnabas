/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.strimzi.operator.cluster.model.pki.SubjectIdentity;

public class CruiseControlIdentity extends SubjectIdentity {

    public static final CruiseControlIdentity INSTANCE = new CruiseControlIdentity();

    private CruiseControlIdentity() {
    }

    @Override
    public boolean equals(Object other) {
        return other == INSTANCE;
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
