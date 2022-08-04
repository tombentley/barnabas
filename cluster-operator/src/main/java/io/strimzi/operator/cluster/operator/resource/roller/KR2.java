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
    final Observer observer;
    final Processor processor;
    private final List<Context> contexts;

    public KR2(int numPods, Observer observer, Processor processor) {
        this.numPods = numPods;
        this.observer = observer;
        this.processor = processor;
        this.contexts = IntStream.range(0, numPods).boxed()
                .map(Context::new)
                .collect(Collectors.toList());
    }

    Future<Void> roll() {

        // TODO still need to support roll-controller last

        while (true) {
            int processed = processOne();
            if (processed == -1) {
                break;
            }
        }
        // TODO close Admin client
        return null;
    }

    int processOne() {
        var observations = observer.observe(contexts);
        contexts.forEach(c -> c.classify(observations.get(c.brokerId())));
        contexts.sort(Comparator.comparing((Context ctx) -> ctx.state().priority())
                .thenComparing(Context::brokerId));
        Context firstContext = contexts.get(0);
        var worstState = firstContext.state();
        final int processed;
        if (worstState.isHealthy()) {
            // Even the worst state is healthy => We're done
            processed = -1;
        } else if (worstState == State.STALE_BLOCKED) {
            // TODO sleep and retry
            processed = -2;
        } else {
            // Get all the contexts whose state is equally the worst
            if (process(firstContext)) {
                firstContext.touched();
            }
            processed = firstContext.brokerId();
        }
        return processed;
    }

    private boolean process(Context context) {
        switch (context.state()) {
            case STALE_RESTARTABLE:
            case UNHEALTHY:
                processor.deletePod(context.brokerId());
                processor.awaitStable(context.brokerId()); // TODO should this actually be a method of Observer?
                return true;
            case STALE_RECONFIGGABLE:
                processor.reconfigure(context.brokerId());
                processor.awaitStable(context.brokerId()); // TODO should this actually be a method of Observer? Do we need stable, or merely healthy?
                return true;
            case RESTARTED:
            case RECOVERY:
            case SYNCING:
                processor.awaitStable(context.brokerId()); // TODO should this actually be a method of Observer? Do we need stable, or merely healthy?
                return false;
            case STALE_BLOCKED:
            case UNKNOWN:
            case STABLE:
            default:
                throw new IllegalStateException("Unexpected state " + context);
        }
    }


}
