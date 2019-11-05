/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaConnectorSpec;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.Status;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.AbstractWatchableResourceOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaConnectAssemblyOperator extends AbstractAssemblyOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> {

    private static final Logger log = LogManager.getLogger(KafkaConnectAssemblyOperator.class.getName());
    public static final String ANNO_STRIMZI_IO_LOGGING = Annotations.STRIMZI_DOMAIN + "/logging";
    private final DeploymentOperator deploymentOperations;
    private final KafkaVersion.Lookup versions;
    private final CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> connectorOperator;
    private final Function<Vertx, KafkaConnectApi> connectClientProvider;

    /**
     * @param vertx The Vertx instance
     * @param pfa Platform features availability properties
     * @param certManager Certificate manager
     * @param supplier Supplies the operators for different resources
     * @param config ClusterOperator configuration. Used to get the user-configured image pull policy and the secrets.
     */
    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        CertManager certManager,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config) {
        this(vertx, pfa, certManager, supplier, config, connect -> new KafkaConnectApiImpl(vertx));
    }

    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        CertManager certManager,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<Vertx, KafkaConnectApi> connectClientProvider) {
        super(vertx, pfa, KafkaConnect.RESOURCE_KIND, certManager, supplier.connectOperator, supplier, config);
        this.deploymentOperations = supplier.deploymentOperations;
        this.versions = config.versions();
        this.connectorOperator = supplier.kafkaConnectorOperator;
        this.connectClientProvider = connectClientProvider;
    }

    @Override
    protected Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        Future<Void> createOrUpdateFuture = Future.future();
        String namespace = reconciliation.namespace();
        String name = reconciliation.name();
        KafkaConnectCluster connect;
        KafkaConnectStatus kafkaConnectStatus = new KafkaConnectStatus();
        if (kafkaConnect.getSpec() == null) {
            log.error("{} spec cannot be null", kafkaConnect.getMetadata().getName());
            return Future.failedFuture("Spec cannot be null");
        }
        try {
            connect = KafkaConnectCluster.fromCrd(kafkaConnect, versions);
        } catch (Exception e) {
            StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, Future.failedFuture(e));
            return this.updateStatus((CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect>) resourceOperator, kafkaConnect, reconciliation,
                    KafkaConnect::getStatus, kafkaConnectStatus,
                (connect1, status) -> {
                    return new KafkaConnectBuilder(connect1).withStatus(status).build();
                });
        }

        ConfigMap logAndMetricsConfigMap = connect.generateMetricsAndLogConfigMap(connect.getLogging() instanceof ExternalLogging ?
                configMapOperations.get(namespace, ((ExternalLogging) connect.getLogging()).getName()) :
                null);

        Map<String, String> annotations = new HashMap<>();
        annotations.put(ANNO_STRIMZI_IO_LOGGING, logAndMetricsConfigMap.getData().get(connect.ANCILLARY_CM_KEY_LOG_CONFIG));

        log.debug("{}: Updating Kafka Connect cluster", reconciliation, name, namespace);
        Future<Void> chainFuture = Future.future();
        connectServiceAccount(namespace, connect)
                .compose(i -> deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas()))
                .compose(scale -> serviceOperations.reconcile(namespace, connect.getServiceName(), connect.generateService()))
                .compose(i -> configMapOperations.reconcile(namespace, connect.getAncillaryConfigName(), logAndMetricsConfigMap))
                .compose(i -> podDisruptionBudgetOperator.reconcile(namespace, connect.getName(), connect.generatePodDisruptionBudget()))
                .compose(i -> deploymentOperations.reconcile(namespace, connect.getName(), connect.generateDeployment(annotations, pfa.isOpenshift(), imagePullPolicy, imagePullSecrets)))
                .compose(i -> deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas()))
                .compose(i -> deploymentOperations.waitForObserved(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> deploymentOperations.readiness(namespace, connect.getName(), 1_000, operationTimeoutMs))
                .compose(i -> reconcileConnectors(reconciliation, namespace, kafkaConnect))
                .compose(i -> chainFuture.complete(), chainFuture)
                .setHandler(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult);
                    kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, KafkaConnectCluster.REST_API_PORT));

                    this.updateStatus((CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect>) resourceOperator, kafkaConnect, reconciliation, KafkaConnect::getStatus, kafkaConnectStatus,
                        (connect1, status) -> {
                            return new KafkaConnectBuilder(connect1).withStatus(status).build();
                        }).setHandler(statusResult -> {
                            // If both features succeeded, createOrUpdate succeeded as well
                            // If one or both of them failed, we prefer the reconciliation failure as the main error
                            if (reconciliationResult.succeeded() && statusResult.succeeded()) {
                                createOrUpdateFuture.complete();
                            } else if (reconciliationResult.failed()) {
                                createOrUpdateFuture.fail(reconciliationResult.cause());
                            } else {
                                createOrUpdateFuture.fail(statusResult.cause());
                            }
                        });
                });
        return createOrUpdateFuture;
    }

    public static <C extends CustomResource,
            T extends HasMetadata,
            S extends AbstractWatchableResourceOperator<?, T, ?, ?, ?>>
        Future<Void> createConnectorWatch(Vertx vertx, CrdOperator<?, C, ?, ?> connectorOperator,
                                          AbstractOperator<T, S> operator, String namespace) {
        return Util.async(vertx, () -> {
            connectorOperator.watch(namespace, new Watcher<C>() {
                @Override
                public void eventReceived(Action action, C kafkaConnector) {
                    // TODO There's nothing stopping a KafkaConnect having the same name as a KafkaConnectS2I
                    // so there's still be possibility of a single KafkaConnector being reconciled twice
                    String kafaConnectCrName = kafkaConnector.getMetadata().getLabels().get("strimzi.io/cluster");
                    if (kafaConnectCrName != null) {
                        // TODO Check whether this KafkaConnect exists & is within the scope of this operator

                        // TODO Should we reconcile the whole thing, or just grab the lock and call reconcileConnectors() ?
                        Reconciliation r = new Reconciliation("watch2", operator.kind(), kafkaConnector.getMetadata().getNamespace(), kafaConnectCrName);
                        operator.reconcile(r);
                    } else {
                        // TODO Update status/log that the label is needed
                    }
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
    private Future<Void> reconcileConnectors(Reconciliation reconciliation, String namespace, KafkaConnect connect) {
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
    private <T extends CustomResource, S extends Status, L extends CustomResourceList<T>, D extends Doneable<T>> Future<Void> updateStatus(CrdOperator<KubernetesClient, T, L, D> resourceOperator,
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

    private Future<ReconcileResult<ServiceAccount>> connectServiceAccount(String namespace, KafkaConnectCluster connect) {
        return serviceAccountOperations.reconcile(namespace,
                KafkaConnectResources.serviceAccountName(connect.getCluster()),
                connect.generateServiceAccount());
    }
}
