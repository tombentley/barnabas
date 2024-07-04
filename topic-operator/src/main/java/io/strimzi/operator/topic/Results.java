/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class Results {
    private final Map<ReconcilableTopic, Either<TopicOperatorException, Object>> results = new HashMap<>();

    public void recordRightResult(ReconcilableTopic key) {
        recordResult(key, Either.ofRight(null));
    }

    public void recordLeftResults(Stream<Pair<ReconcilableTopic, TopicOperatorException>> errors) {
        errors.forEach(pair -> recordResult(pair.getKey(), Either.ofLeft(pair.getValue())));
    }

    public void recordRightResults(Stream<? extends Pair<ReconcilableTopic, ? extends Object>> ok) {
        ok.forEach(pair -> recordResult(pair.getKey(), Either.ofRight(null)));
    }

    public void recordRightResults(Collection<ReconcilableTopic> ok) {
        ok.forEach(rt -> recordResult(rt, Either.ofRight(null)));
    }

    private void recordResult(ReconcilableTopic key,
                             Either<TopicOperatorException, Object> result) {
        results.compute(key, (k, v) -> {
            if (v == null) { // use given result if there is no existing result
                return result;
            } else if (v.isRight()) { // if the existing result was success use the given result
                return result;
            } else { // otherwise the existing result must be an error, and "first error wins"
                return v;
            }
        });
    }

    public int size() {
        return results.size();
    }

    public void forEachRightResult(BiConsumer<ReconcilableTopic, Object> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (either.isRight()) {
                action.accept(reconcilableTopic, either.right());
            }
        });
    }

    public void forEachLeftResult(BiConsumer<ReconcilableTopic, TopicOperatorException> action) {
        results.forEach((reconcilableTopic, either) -> {
            if (!either.isRight()){
                action.accept(reconcilableTopic, either.left());
            }
        });
    }

    public void addAll(Results results) {
        for (var entry : results.results.entrySet()) {
            this.recordResult(entry.getKey(), entry.getValue());
        }
    }
}
