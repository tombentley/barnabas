/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
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
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.StatusDiff;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.NamespaceAndName;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final Function<KafkaConnect, KafkaConnectApi> connectClientProvider;
    private final Map<String, WatchValue> connectorWatches = new ConcurrentHashMap<>();

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
        this(vertx, pfa, certManager, supplier, config, connect -> new KafkaConnectApiImpl(vertx, KafkaConnectResources.serviceName(connect.getMetadata().getName()), 8083));
    }

    public KafkaConnectAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa,
                                        CertManager certManager,
                                        ResourceOperatorSupplier supplier,
                                        ClusterOperatorConfig config,
                                        Function<KafkaConnect, KafkaConnectApi> connectClientProvider) {
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
            return updateStatus(kafkaConnect, reconciliation, kafkaConnectStatus);
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
                .compose(i -> maybeWatchConnectors(reconciliation, kafkaConnect))
                .compose(i -> chainFuture.complete(), chainFuture)
                .setHandler(reconciliationResult -> {
                    StatusUtils.setStatusConditionAndObservedGeneration(kafkaConnect, kafkaConnectStatus, reconciliationResult);
                    kafkaConnectStatus.setUrl(KafkaConnectResources.url(connect.getCluster(), namespace, KafkaConnectCluster.REST_API_PORT));

                    updateStatus(kafkaConnect, reconciliation, kafkaConnectStatus).setHandler(statusResult -> {
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

    /**
     * Creates a watch using the {@code KafkaConnect.spec.connectorSelector} to watch connectors in
     * this connect instance.
     * @param reconciliation The KafkaConnect reconciliation
     * @param kafkaConnect The resource being reconciled
     * @return Future which completes once the connectors are being watched
     */
    private Future<Void> maybeWatchConnectors(Reconciliation reconciliation, KafkaConnect kafkaConnect) {
        LabelSelector connectorSelector = kafkaConnect.getSpec().getConnectorSelector();
        if (connectorSelector != null) {
            log.debug("{}: resource uses selector {}}", reconciliation, connectorSelector);
            return connectorWatches.compute(watchKey(reconciliation), (k, currentValue) -> {
                if (currentValue != null) {
                    if (connectorSelector.equals(currentValue.selector)) {
                        log.debug("{}: There's an existing watch for selector {}}",
                                reconciliation, connectorSelector);
                        return currentValue;
                    } else {
                        log.debug("{}: selector has changed from {} to {}, replacing watch",
                                reconciliation, currentValue.selector, connectorSelector);
                    }
                } else {
                    log.debug("{}: Creating new watch for selector {}}", reconciliation, connectorSelector);
                }
                return setupWatch(reconciliation, kafkaConnect, currentValue);
            }).watch.map((Void) null);
        } else {
            log.debug("{}: resource doesn't use a selector => not watching for KafkaConnectors", reconciliation);
            return Future.succeededFuture();
        }
    }

    @SuppressWarnings("unchecked")
    private WatchValue setupWatch(Reconciliation reconciliation, KafkaConnect kafkaConnect, WatchValue currentWatchValue) {
        if (currentWatchValue != null) {
            currentWatchValue.asyncClose();
        }
        LabelSelector connectorSelector = kafkaConnect.getSpec().getConnectorSelector();
        KafkaConnectorAssemblyOperator operator = new KafkaConnectorAssemblyOperator(vertx, connectorSelector, connectorOperator,
                connectClientProvider.apply(kafkaConnect));
        WatchValue watchValue = new WatchValue(reconciliation, operator);
        if (currentWatchValue != null) {
            // It's not enough to reconcile the _new_ connector operator because that's using the new selector
            // it won't find the connectors which matched the old selector but which don't match the new one.
            CompositeFuture.join(
                    connectorOperator.listAsync(reconciliation.namespace(), currentWatchValue.operator.selector()),
                    connectorOperator.listAsync(reconciliation.namespace(), operator.selector()))
                .compose(joinedFuture -> {
                    Set<NamespaceAndName> oldConnectors = ((List<KafkaConnector>) joinedFuture.resultAt(0)).stream()
                        .map(connector -> new NamespaceAndName(reconciliation.namespace(), connector.getMetadata().getName()))
                        .collect(Collectors.toSet());
                    Set<NamespaceAndName> newConnectors = ((List<KafkaConnector>) joinedFuture.resultAt(1)).stream()
                        .map(connector -> new NamespaceAndName(reconciliation.namespace(), connector.getMetadata().getName()))
                        .collect(Collectors.toSet());
                    oldConnectors.removeAll(newConnectors);
                    // We have to call delete() directly, because reconcile() doesn't test resources against
                    // the operators selector.
                    for (NamespaceAndName ref : oldConnectors) {
                        operator.delete(new Reconciliation("", operator.kind(), ref.getNamespace(), ref.getName()));
                    }
                    Future<Void> f = Future.future();
                    operator.reconcileThese("selector-change",
                        newConnectors,
                        f);
                    return f;
                });
        }
        return watchValue;
    }

    class WatchValue {
        final LabelSelector selector;
        final Future<Watch> watch;
        final KafkaConnectorAssemblyOperator operator;

        public WatchValue(Reconciliation reconciliation, KafkaConnectorAssemblyOperator operator) {
            this.selector = operator.selector().get();
            this.operator = operator;
            this.watch = operator.createWatch(reconciliation.namespace(), operator.recreateWatch(reconciliation.namespace()));
        }

        Future<Void> asyncClose() {
            return watch.compose(watch -> async(() -> {
                log.trace("Closing {} on selector {}", watch, selector);
                watch.close();
                log.trace("Closed {} on selector {}", watch, selector);
                return null;
            }));
        }
    }

    private String watchKey(Reconciliation reconciliation) {
        return reconciliation.namespace() + "/" + reconciliation.name();
    }

    @Override
    protected Future<Boolean> delete(Reconciliation reconciliation) {
        WatchValue watchValue = connectorWatches.remove(watchKey(reconciliation));
        Future<Void> compose = watchValue != null ? watchValue.asyncClose() : Future.succeededFuture();
        return compose.compose(ignored -> super.delete(reconciliation));
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
    private Future<Void> updateStatus(KafkaConnect kafkaConnectAssembly, Reconciliation reconciliation, KafkaConnectStatus desiredStatus) {
        Future<Void> updateStatusFuture = Future.future();

        resourceOperator.getAsync(kafkaConnectAssembly.getMetadata().getNamespace(), kafkaConnectAssembly.getMetadata().getName()).setHandler(getRes -> {
            if (getRes.succeeded()) {
                KafkaConnect connect = getRes.result();

                if (connect != null) {
                    if (StatusUtils.isResourceV1alpha1(connect)) {
                        log.warn("{}: The resource needs to be upgraded from version {} to 'v1beta1' to use the status field", reconciliation, connect.getApiVersion());
                        updateStatusFuture.complete();
                    } else {
                        KafkaConnectStatus currentStatus = connect.getStatus();

                        StatusDiff ksDiff = new StatusDiff(currentStatus, desiredStatus);

                        if (!ksDiff.isEmpty()) {
                            KafkaConnect resourceWithNewStatus = new KafkaConnectBuilder(connect).withStatus(desiredStatus).build();

                            ((CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect>) resourceOperator).updateStatusAsync(resourceWithNewStatus).setHandler(updateRes -> {
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
