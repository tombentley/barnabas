/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;


import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Report;
import net.jqwik.api.Reporting;
import net.jqwik.api.arbitraries.IntegerArbitrary;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.stateful.Action;
import net.jqwik.api.stateful.ActionSequence;

class KR2Test {

    static Arbitrary<Action<KR2>> observation() {
        IntegerArbitrary brokerIdArbitrary = Arbitraries.integers().between(1, 5);
        Arbitrary<Integer> e = Arbitraries.integers().between(0, 1 << 8);
        return Combinators.combine(brokerIdArbitrary, e).as(
                (x1, x2) -> new ObservationAction(x1,
                        new Observation((x2 & 0x1) == 0,
                                (x2 >> 1 & 0x1) == 0,
                                (x2 >> 2 & 0x1) == 0,
                                (x2 >> 3 & 0x1) == 0,
                                (x2 >> 4 & 0x1) == 0,
                                (x2 >> 5 & 0x1) == 0,
                                (x2 >> 6 & 0x1) == 0,
                                (x2 >> 7 & 0x1) == 0))
        );
    }

    static Arbitrary<Action<KR2>> actions() {
        return Arbitraries.oneOf(observation());
    }

    @Provide
    Arbitrary<ActionSequence<KR2>> sequences() {
        return Arbitraries.sequences(observation());
    }

    @Property
    @Report(Reporting.GENERATED)
    void never_touch_a_broker_more_than_once(
            @ForAll @IntRange(min = 1, max = 4) int numPods,
            @ForAll("sequences") ActionSequence<KR2> sequence) {
        var observations = IntStream.range(0, numPods).boxed()
                .collect(Collectors.toMap(i -> i, i -> new Observation(true, true, true,true, true, true, true, true)));
        KR2 roller = new KR2(numPods, new MockObserver(observations), new MockProcessor(observations));
        sequence.run(roller);
    }


}