/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.model.crd.Kafka;
import io.strimzi.controller.cluster.model.crd.KafkaAssembly;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaCluster extends AbstractModel {

    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "clients";

    protected static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "replication";

    private static final String NAME_SUFFIX = "-kafka";
    private static final String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static final String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_IMAGE", "strimzi/kafka:latest");

    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    // Kafka configuration defaults
    private static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";
    private static final int DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 3;

    // Configuration keys
    public static final String KEY_IMAGE = "kafka-image";
    public static final String KEY_REPLICAS = "kafka-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "kafka-metrics-config";
    public static final String KEY_STORAGE = "kafka-storage";
    public static final String KEY_CPU_LIMIT = "kafka-cpu-limit";
    public static final String KEY_CPU_REQUEST = "kafka-cpu-request";
    public static final String KEY_MEMORY_LIMIT = "kafka-memory-limit";
    public static final String KEY_MEMORY_REQUEST = "kafka-memory-request";

    // Kafka configuration keys
    public static final String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    public static final String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    public static final String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    public static final String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";
    private static final String KEY_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka cluster resources are going to be created
     * @param cluster  overall cluster name
     */
    private KafkaCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = kafkaClusterName(cluster);
        this.headlessName = headlessName(cluster);
        this.metricsConfigName = metricConfigsName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/kafka/kafka_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";
        this.metricsConfigVolumeName = "kafka-metrics-config";
        this.metricsConfigMountPath = "/opt/prometheus/config/";
    }

    public static String kafkaClusterName(String cluster) {
        return cluster + KafkaCluster.NAME_SUFFIX;
    }

    public static String metricConfigsName(String cluster) {
        return cluster + KafkaCluster.METRICS_CONFIG_SUFFIX;
    }

    public static String headlessName(String cluster) {
        return cluster + KafkaCluster.HEADLESS_NAME_SUFFIX;
    }

    public static String kafkaPodName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
    }

    /**
     * Create a Kafka cluster from the related ConfigMap resource
     *
     * @param kafkaClusterCm ConfigMap with cluster configuration
     * @return Kafka cluster instance
     */
    public static KafkaCluster fromConfigMap(ConfigMap kafkaClusterCm) {
        KafkaCluster kafka = new KafkaCluster(kafkaClusterCm.getMetadata().getNamespace(),
                kafkaClusterCm.getMetadata().getName(),
                Labels.fromResource(kafkaClusterCm));

        Map<String, String> data = kafkaClusterCm.getData();
        kafka.setReplicas(Integer.parseInt(data.getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafka.setImage(data.getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafka.setHealthCheckTimeout(Integer.parseInt(data.getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafka.setZookeeperConnect(data.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, kafkaClusterCm.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(data.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        String metricsConfig = data.get(KEY_METRICS_CONFIG);
        kafka.setMetricsEnabled(metricsConfig != null);
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfig(new JsonObject(metricsConfig));
        }

        String storageConfig = data.get(KEY_STORAGE);
        kafka.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        kafka.setCpuLimit(data.get(KEY_CPU_LIMIT));
        kafka.setCpuRequest(data.get(KEY_CPU_REQUEST));
        kafka.setMemoryLimit(data.get(KEY_MEMORY_LIMIT));
        kafka.setMemoryRequest(data.get(KEY_MEMORY_REQUEST));

        return kafka;
    }

    public static KafkaCluster fromCrd(KafkaAssembly crd) {
        KafkaCluster result = new KafkaCluster(crd.getMetadata().getNamespace(),
                crd.getMetadata().getName(),
                Labels.fromResource(crd));
        Kafka kafka = crd.getKafka();
        result.setReplicas(kafka.getReplicas());
        result.setImage(kafka.getImage());
        result.setHealthCheckInitialDelay(kafka.getReadinessProbe().getInitialDelaySeconds());
        result.setHealthCheckTimeout(kafka.getReadinessProbe().getTimeoutSeconds());
        // TODO Fix this ugly mess with the two Storages
        Storage s = new Storage(Storage.StorageType.from(kafka.getStorage().getType()));
        s.withClass(kafka.getStorage().getStorageClass());
        s.withDeleteClaim(kafka.getStorage().isDeleteClaim());
        s.withSelector(new LabelSelector(null, kafka.getStorage().getSelector())); // TODO
        s.withSize(new Quantity("" + Memory.parseMemory(kafka.getStorage().getSize())));
        result.setStorage(s);
        return result;
    }

    /**
     * Create a Kafka cluster from the deployed StatefulSet resource
     *
     * @param ss The StatefulSet from which the cluster state should be recovered.
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Kafka cluster instance
     */
    public static KafkaCluster fromAssembly(StatefulSet ss, String namespace, String cluster) {

        KafkaCluster kafka =  new KafkaCluster(namespace, cluster, Labels.fromResource(ss));

        kafka.setReplicas(ss.getSpec().getReplicas());
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafka.setImage(container.getImage());
        kafka.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(container);

        kafka.setZookeeperConnect(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        kafka.setMetricsEnabled(Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED))));
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfigName(metricConfigsName(cluster));
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {

            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                storage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
            kafka.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            kafka.setStorage(storage);
        }

        return kafka;
    }



    private List<ServicePort> getServicePorts() {
        List<ServicePort> ports = new ArrayList<>(2);
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));
        return ports;
    }

    /**
     * Generates a Service according to configured defaults
     * @return The generated Service
     */
    public Service generateService() {

        return createService("ClusterIP", getServicePorts());
    }

    /**
     * Generates a headless Service according to configured defaults
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(headlessName, getServicePorts(), annotations);
    }

    /**
     * Generates a StatefulSet according to configured defaults
     * @param isOpenShift True iff this controller is operating within OpenShift.
     * @return The generate StatefulSet
     */
    public StatefulSet generateStatefulSet(boolean isOpenShift) {

        return createStatefulSet(
                getContainerPortList(),
                getVolumes(),
                getVolumeClaims(),
                getVolumeMounts(),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                resources(),
                isOpenShift);
    }


    /**
     * Generates a metrics ConfigMap according to configured defaults
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsConfigMap() {
        if (isMetricsEnabled()) {
            Map<String, String> data = new HashMap<>();
            data.put(METRICS_CONFIG_FILE, getMetricsConfig().toString());
            return createConfigMap(getMetricsConfigName(), data);
        } else {
            return null;
        }
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(3);
        portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
        portList.add(createContainerPort(REPLICATION_PORT_NAME, REPLICATION_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(metricsPortName, metricsPort, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage.type() == Storage.StorageType.EPHEMERAL) {
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }
        if (isMetricsEnabled) {
            volumeList.add(createConfigMapVolume(metricsConfigVolumeName, metricsConfigName));
        }

        return volumeList;
    }

    private List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        if (storage.type() == Storage.StorageType.PERSISTENT_CLAIM) {
            pvcList.add(createPersistentVolumeClaim(VOLUME_NAME));
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(VOLUME_NAME, mountPath));
        if (isMetricsEnabled) {
            volumeMountList.add(createVolumeMount(metricsConfigVolumeName, metricsConfigMountPath));
        }

        return volumeMountList;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(KEY_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(defaultReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(offsetsTopicReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(transactionStateLogReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));

        return varList;
    }

    protected void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    protected void setDefaultReplicationFactor(int defaultReplicationFactor) {
        this.defaultReplicationFactor = defaultReplicationFactor;
    }

    protected void setOffsetsTopicReplicationFactor(int offsetsTopicReplicationFactor) {
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
    }

    protected void setTransactionStateLogReplicationFactor(int transactionStateLogReplicationFactor) {
        this.transactionStateLogReplicationFactor = transactionStateLogReplicationFactor;
    }
}