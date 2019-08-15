/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.client;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import io.strimzi.api.kafka.model.status.Condition;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Encapsulate the overriding of readiness-related methods
 * @param <T> The resource type
 * @param <L> The resource list type
 * @param <D> The doneable type
 * @param <R> The resource operations
 */
public abstract class StrimziReadyOperationsImpl<
            T extends HasMetadata,
            L extends KubernetesResourceList,
            D extends Doneable<T>,
            R extends Resource<T, D>>
        extends HasMetadataOperation<T, L, D, R>
        implements Resource<T, D> {

    public StrimziReadyOperationsImpl(OperationContext ctx) {
        super(ctx);
    }

    protected abstract boolean isReady(T resource);

    @Override
    public Boolean isReady() {
        return isReady(get());
    }

    @Override
    protected T periodicWatchUntilReady(int i, long started, long interval, long amount) {
        T item = fromServer().get();
        if (isReady(item)) {
            return item;
        }

        ReadinessWatcher<T> watcher = new ReadinessWatcher<>(this, item);
        try (Watch watch = watch(item.getMetadata().getResourceVersion(), watcher)) {
            try {
                return watcher.await(interval, TimeUnit.NANOSECONDS);
            } catch (KubernetesClientTimeoutException e) {
                if (i <= 0) {
                    throw e;
                }
            }

            long remaining =  (started + amount) - System.nanoTime();
            long next = Math.max(0, Math.min(remaining, interval));
            return periodicWatchUntilReady(i - 1, started, next, amount);
        }
    }

    @Override
    public T waitUntilReady(long amount, TimeUnit timeUnit) throws InterruptedException {

        long started = System.nanoTime();
        waitUntilExists(amount, timeUnit);
        long alreadySpent = System.nanoTime() - timeUnit.toNanos(started);

        long remaining = timeUnit.toNanos(amount) - alreadySpent;
        if (remaining <= 0) {
            return periodicWatchUntilReady(0, System.nanoTime(), 0, 0);
        }

        return periodicWatchUntilReady(10, System.nanoTime(), Math.max(remaining / 10, 1000L), remaining);

    }

    protected static Predicate<Condition> containsReadyCondition() {
        return condition ->
                "Ready".equals(condition.getType())
                        && "True".equals(condition.getStatus());
    }
}
