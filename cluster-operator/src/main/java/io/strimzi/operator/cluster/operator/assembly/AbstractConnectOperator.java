/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.AbstractResourceOperator;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractConnectOperator<C extends KubernetesClient, T extends HasMetadata,
        L extends KubernetesResourceList/*<T>*/, D extends Doneable<T>, R extends Resource<T, D>>
        extends AbstractAssemblyOperator<C, T, L, D, R> {

    private static final Logger log = LogManager.getLogger(AbstractConnectOperator.class.getName());

    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> connectorOperator;
    private final Function<Vertx, KafkaConnectApi> connectClientProvider;

    public AbstractConnectOperator(Vertx vertx, PlatformFeaturesAvailability pfa, String kind, CertManager certManager,
                                   AbstractWatchableResourceOperator<C, T, L, D, R> resourceOperator,
                                   ResourceOperatorSupplier supplier, ClusterOperatorConfig config,
                                   Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, kind, certManager, resourceOperator, supplier, config);
        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectClientProvider = connectClientProvider;
    }

    /**
     *
     * @param vertx The vertx instance
     * @param connectorOperations The connector operator
     * @param connectOperations The connector operations
     * @param connectOperator The connect operator
     * @param namespace The namespace
     * @param <T> The type of the connect resource (e.g. KafkaConnect or KafkaConnectS2I)
     * @param <S> The type of the connector operator
     * @return
     */
    public static <
            T extends HasMetadata,
            S extends AbstractWatchableResourceOperator<?, T, ?, ?, ?>>
        Future<Void> createConnectorWatch(Vertx vertx, CrdOperator<?, KafkaConnector, ?, ?> connectorOperations,
                                          AbstractResourceOperator<?, T, ?, ?, ?> connectOperations,
                                          AbstractOperator<T, S> connectOperator, String namespace) {
        return Util.async(vertx, () -> {
            connectorOperations.watch(namespace, new Watcher<KafkaConnector>() {
                @Override
                public void eventReceived(Action action, KafkaConnector kafkaConnector) {
                    // TODO There's nothing stopping a KafkaConnect having the same name as a KafkaConnectS2I
                    // so there's still be possibility of a single KafkaConnector being reconciled twice
                    String kafkaConnectCrName = kafkaConnector.getMetadata().getLabels().get("strimzi.io/cluster");
                    Future<Void> f;
                    if (kafkaConnectCrName != null) {
                        // Check whether this KafkaConnect exists & is within the scope of this operator
                        f = connectOperations.getAsync(namespace, kafkaConnectCrName).compose(t -> {
                            if (t == null) {
                                return Future.failedFuture(new NoSuchResourceException(
                                        "KafkaConnector resource '" + kafkaConnectCrName + "' identified by label 'strimzi.io/cluster' does not exist in namespace " + namespace + "."));
                            } else {
                                // TODO Should we reconcile the whole thing, or just grab the lock and call reconcileConnectors() ?
                                Reconciliation r = new Reconciliation("connector-watch", connectOperator.kind(), kafkaConnector.getMetadata().getNamespace(), kafkaConnectCrName);
                                return connectOperator.reconcile(r);
                            }
                        });
                    } else {
                        f = Future.failedFuture(new InvalidResourceException("Resource lacks 'strimzi.io/cluster' label: No connect cluster in which to create this connector."));
                    }
                    f.setHandler(ar -> {
                        KafkaConnectorStatus status = new KafkaConnectorStatus();
                        StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnector, status, ar);
                        StatusDiff diff = new StatusDiff(kafkaConnector.getStatus(), status);
                        if (!diff.isEmpty()) {
                            KafkaConnector copy = new KafkaConnectorBuilder(kafkaConnector).build();
                            copy.setStatus(status);
                            connectorOperations.updateStatusAsync(copy);
                        }
                    });
                }

                @Override
                public void onClose(KubernetesClientException e) {
                    if (e != null) {
                        throw e;
                    }
                }
            });
            return null;
        });
    }

    // TODO the following code would be shared between Connect and ConnectS2I, that's super fragile. We probably need a common base class for those operators
    protected Future<Void> reconcileConnectors(Reconciliation reconciliation, String namespace, T connect) {
        String connectName = connect.getMetadata().getName();
        String host = KafkaConnectResources.serviceName(connect.getMetadata().getName());
        KafkaConnectApi apiClient = connectClientProvider.apply(vertx);
        return CompositeFuture.join(apiClient.list(host, KafkaConnectCluster.REST_API_PORT),
                connectorOperator.listAsync(namespace,
                        Optional.of(new LabelSelectorBuilder().addToMatchLabels("strimzi.io/cluster", connectName).build()))
        ).compose(cf -> {
            List<String> runningConnectors = cf.resultAt(0);
            List<KafkaConnector> desired = cf.resultAt(1);

            Set<String> delete = new HashSet<>(runningConnectors);
            delete.removeAll(desired.stream().map(c -> c.getMetadata().getName()).collect(Collectors.toSet()));
            Stream<Future<Void>> deletionFutures = delete.stream().map(connector -> apiClient.delete(host, KafkaConnectCluster.REST_API_PORT, connector));
            Stream<Future<Void>> createUpdateFutures = desired.stream()
                    .map(connector -> {

                        return apiClient.createOrUpdatePutRequest(host, KafkaConnectCluster.REST_API_PORT, connector.getMetadata().getName(), asJson(connector))
                                .compose(i -> updateConnectorStatus(reconciliation, connector, null))
                                .recover(e -> updateConnectorStatus(reconciliation, connector, e));
                    });
            return CompositeFuture.join(Stream.concat(deletionFutures, createUpdateFutures).collect(Collectors.toList())).map((Void) null);
        });
    }

    Future<Void> updateConnectorStatus(Reconciliation reconciliation, KafkaConnector connector, Throwable error) {
        KafkaConnectorStatus status = connector.getStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(connector, status, error != null ? Future.failedFuture(error) : Future.succeededFuture());
        return updateStatus(connectorOperator, connector, reconciliation, KafkaConnector::getStatus, status,
            (connector1, status1) -> {
                return new KafkaConnectorBuilder(connector1).withStatus(status1).build();
            });
    }

    private JsonObject asJson(KafkaConnector assemblyResource) {
        KafkaConnectorSpec spec = assemblyResource.getSpec();
        JsonObject connectorConfigJson = new JsonObject();
        if (spec.getConfig() != null) {
            for (Map.Entry<String, Object> cf : spec.getConfig().entrySet()) {
                String name = cf.getKey();
                if ("connector.class".equals(name)
                        || "tasks.max".equals(name)) {
                    // TODO include resource namespace and name in this message
                    log.warn("Configuration parameter {} in KafkaConnector.spec.config will be ignored and the value from KafkaConnector.spec will be used instead",
                            name);
                }
                connectorConfigJson.put(name, cf.getValue());
            }
        }
        return connectorConfigJson
                .put("connector.class", spec.getClassName())
                .put("tasks.max", spec.getTasksMax());
    }

    /**
     * Updates the Status field of the Kafka Connect CR. It diffs the desired status against the current status and calls
     * the update only when there is any difference in non-timestamp fields.
     *
     * @param kafkaConnectAssembly The CR of Kafka Connect
     * @param reconciliation Reconciliation information
     * @param desiredStatus The KafkaConnectStatus which should be set
     *
     * @return
     */
    protected <T extends CustomResource, S extends Status, L extends CustomResourceList<T>, D extends Doneable<T>> Future<Void> updateStatus(CrdOperator<KubernetesClient, T, L, D> resourceOperator,
                                                                                                                                           T kafkaConnectAssembly,
                                                                                                                                           Reconciliation reconciliation,
                                                                                                                                           Function<T, S> fn,
                                                                                                                                           S desiredStatus,
                                                                                                                                           BiFunction<T, S, T> copyWithStatus) {
        Future<Void> updateStatusFuture = Future.future();

        resourceOperator.getAsync(kafkaConnectAssembly.getMetadata().getNamespace(), kafkaConnectAssembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                T connect = getRes.result();

                if (connect != null) {
                    if (StatusUtils.isResourceV1alpha1(connect)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, connect.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        S currentStatus = fn.apply(connect);

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            T resourceWithNewStatus = copyWithStatus.apply(connect, desiredStatus);

                            resourceOperator.updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
                                if (updateRes.succeeded()) {
                                    log.debug("{}: Completed status update", reconciliation);
                                    updateStatusFuture.complete();
                                } else {
                                    log.error("{}: Failed to update status", reconciliation, updateRes.cause());
                                    updateStatusFuture.fail(updateRes.cause());
                                }
                            });
                        } else {
                            log.debug("{}: Status did not change", reconciliation);
                            updateStatusFuture.complete();
                        }
                    }
                } else {
                    log.error("{}: Current Kafka Connect resource not found", reconciliation);
                    updateStatusFuture.fail("Current Kafka Connect resource not found");
                }
            } else {
                log.error("{}: Failed to get the current Kafka Connect resource and its status", reconciliation, getRes.cause());
                updateStatusFuture.fail(getRes.cause());
            }
        });

        return updateStatusFuture;
    }
}
