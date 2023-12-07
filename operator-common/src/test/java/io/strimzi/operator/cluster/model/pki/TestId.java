/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Objects;

class TestId extends SubjectIdentity {

    private final String name;

    public TestId(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestId textId = (TestId) o;
        return Objects.equals(name, textId.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "TestId(" +
                "name='" + name + '\'' +
                ')';
    }

    public String name() {
        return name;
    }
}
