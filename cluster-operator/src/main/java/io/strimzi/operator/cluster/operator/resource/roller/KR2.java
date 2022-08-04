/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.Future;

/**
 * Encapsulates the rolling algorithm in {@link #roll()}.
 */
public class KR2 {

    private final int numPods;
    private final Observer observer;
    private final Processor processor;

    public KR2(int numPods, Observer observer, Processor processor) {
        this.numPods = numPods;
        this.observer = observer;
        this.processor = processor;
    }

    Future<Void> roll() {

        // TODO still need to support roll-controller last
        List<Context> contexts = IntStream.rangeClosed(0, numPods).boxed()
                .map(Context::new)
                .collect(Collectors.toList());
        OUTER: while (true) {
            var observations = observer.observe(contexts);
            contexts.forEach(c -> c.classify(observations.get(c.brokerId())));
            contexts.sort(Comparator.comparing((Context ctx) -> ctx.state().priority())
                    .thenComparing(Context::brokerId));
            Context firstContext = contexts.get(0);
            var worstState = firstContext.state();
            if (worstState.isHealthy()) {
                // Even the worst state is healthy => We're done
                break;
            } else if (worstState == State.STALE_BLOCKED) {
                // TODO sleep and retry
                continue OUTER;
            }
            // Get all the contexts whose state is equally the worst
            processor.process(firstContext);

        }
        // TODO close Admin client
        return null;
    }


}
