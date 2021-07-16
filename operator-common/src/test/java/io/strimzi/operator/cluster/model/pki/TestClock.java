/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A mutable Clock for test purposes.
 */
public class TestClock extends Clock {

    public static final TestClock EPOCH = new TestClock(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));

    private final Clock origin;
    private Clock current;

    public TestClock(Clock origin) {
        this.origin = origin;
        this.current = origin;
    }

    public TestClock advanceFromCurrent(Duration d) {
        current = Clock.offset(current, d);
        return this;
    }

    public TestClock advanceFromOrigin(Duration d) {
        current = Clock.offset(origin, d);
        return this;
    }

    @Override
    public ZoneId getZone() {
        return current.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return current.withZone(zone);
    }

    @Override
    public Instant instant() {
        return current.instant();
    }
}
