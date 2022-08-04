/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.roller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigResource;

class ObserverImpl implements Observer {
    @Override
    public Map<Integer, Observation> observe(List<Context> contexts) {
        try {
            // TODO get pod status from kube
            KubernetesClient client = null;
            var pods = client.pods().inNamespace("").withLabel("", "").list().getItems();

            // TODO get metadata from kafka
            Admin admin = null;
            var nodes = admin.describeCluster().nodes().get();
            // TODO get broker config from Kafka
            var configResult = admin.describeConfigs(contexts.stream()
                    .flatMap(ctx ->
                            Stream.of(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(ctx.brokerId())),
                                    new ConfigResource(ConfigResource.Type.BROKER_LOGGER, Integer.toString(ctx.brokerId()))))
                    .collect(Collectors.toList())).all().get();

            // TODO get broker state from metrics

            // TODO combine results
            var result = new HashMap<Integer, Observation>();
            for (var context : contexts) {
                var pod = pods.stream().filter(p -> brokerIdFromPodName(p.getMetadata().getName()) == context.brokerId()).findFirst();
                var brokerConfig = configResult.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(context.brokerId())));
                var loggerConfig = configResult.get(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, Integer.toString(context.brokerId())));
                var node = nodes.stream().filter(n -> n.id() == context.brokerId()).findFirst().orElse(null);
                if (result.put(context.brokerId(), Observation.create(pod.get(), node, brokerConfig, loggerConfig)) != null) {
                    throw new RuntimeException("Duplicate context");
                }
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private int brokerIdFromPodName(String name) {
        throw new RuntimeException("Not implemented");
    }
}
