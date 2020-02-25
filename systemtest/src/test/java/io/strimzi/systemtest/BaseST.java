/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.interfaces.TestSeparator;
import io.strimzi.systemtest.kafkaclients.ClientFactory;
import io.strimzi.systemtest.kafkaclients.EClientType;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.externalClients.KafkaClient;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static io.strimzi.test.TestUtils.entry;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestExecutionWatcher.class)
public abstract class BaseST implements TestSeparator {

    protected KubeClusterResource cluster = KubeClusterResource.getInstance();
    protected KafkaClient externalBasicKafkaClient = (KafkaClient) ClientFactory.getClient(EClientType.BASIC);
    protected InternalKafkaClient internalKafkaClient = (InternalKafkaClient) ClientFactory.getClient(EClientType.INTERNAL);

    protected static final String CLUSTER_NAME = "my-cluster";

    protected static TimeMeasuringSystem timeMeasuringSystem = TimeMeasuringSystem.getInstance();

    private static final Logger LOGGER = LogManager.getLogger(BaseST.class);
    protected static final String KAFKA_IMAGE_MAP = "STRIMZI_KAFKA_IMAGES";
    protected static final String KAFKA_CONNECT_IMAGE_MAP = "STRIMZI_KAFKA_CONNECT_IMAGES";
    protected static final String KAFKA_MIRROR_MAKER_2_IMAGE_MAP = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    protected static final String TO_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    protected static final String UO_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    protected static final String KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    protected static final String TLS_SIDECAR_ZOOKEEPER_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ZOOKEEPER_IMAGE";
    protected static final String TLS_SIDECAR_KAFKA_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE";
    protected static final String TLS_SIDECAR_EO_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    protected static final String TEST_TOPIC_NAME = "test-topic";
    protected static final String CONSUMER_GROUP_NAME = "my-consumer-group";
    private static final String CLUSTER_OPERATOR_PREFIX = "strimzi";

    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    public static final String HELM_CHART = "../helm-charts/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";

    protected String testClass;
    protected String testName;

    protected Random rng = new Random();

    public static final int MESSAGE_COUNT = 100;
    public static final String TOPIC_NAME = "my-topic";
    public static final String USER_NAME = "user-name-example";

    private HelmClient helmClient() {
        return cluster.helmClient().namespace(cluster.getNamespace());
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, List<String> namespaces, String... resources) {
        cluster.createNamespaces(clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        cluster.applyClusterOperatorInstallFiles();
        // This is needed in case you are using internal kubernetes registry and you want to pull images from there
        for (String namespace : namespaces) {
            Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, false, false);
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, String... resources) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    protected void prepareEnvForOperator(String clientNamespace) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Clear cluster from all created namespaces and configurations files for cluster operator.
     */
    protected void teardownEnvForOperator() {
        cluster.deleteClusterOperatorInstallFiles();
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     */
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
        recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     * @param operationTimeout timeout for CO operations
     */
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces, long operationTimeout) {
        ResourceManager.deleteMethodResources();
        ResourceManager.deleteClassResources();

        KubeClusterResource.getInstance().deleteClusterOperatorInstallFiles();
        KubeClusterResource.getInstance().deleteNamespaces();

        KubeClusterResource.getInstance().createNamespaces(coNamespace, bindingsNamespaces);
        KubeClusterResource.getInstance().applyClusterOperatorInstallFiles();

        ResourceManager.setClassResources();

        applyRoleBindings(coNamespace, bindingsNamespaces);
        // 050-Deployment
        KubernetesResource.clusterOperator(coNamespace).done();
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            // 020-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 021-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding("../install/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 030-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding("../install/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace, bindingsNamespace);
            // 031-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
            // 032-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyRoleBindings(String namespace) {
        applyRoleBindings(namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, String... bindingsNamespaces) {
        applyRoleBindings(namespace, Arrays.asList(bindingsNamespaces));
    }

    protected void assertResources(String namespace, String podName, String containerName, String memoryLimit, String cpuLimit, String memoryRequest, String cpuRequest) {
        Pod po = kubeClient().getPod(podName);
        assertThat("Not found an expected pod  " + podName + " in namespace " + namespace + " but found " +
            kubeClient().listPods().stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), po, is(notNullValue()));

        Optional optional = po.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName)).findFirst();
        assertThat("Not found an expected container " + containerName, optional.isPresent(), is(true));

        Container container = (Container) optional.get();
        Map<String, Quantity> limits = container.getResources().getLimits();
        assertThat(limits.get("memory").getAmount(), is(memoryLimit));
        assertThat(limits.get("cpu").getAmount(), is(cpuLimit));
        Map<String, Quantity> requests = container.getResources().getRequests();
        assertThat(requests.get("memory").getAmount(), is(memoryRequest));
        assertThat(requests.get("cpu").getAmount(), is(cpuRequest));
    }

    private void assertCmdOption(List<String> cmd, String expectedXmx) {
        if (!cmd.contains(expectedXmx)) {
            fail("Failed to find argument matching " + expectedXmx + " in java command line " +
                cmd.stream().collect(Collectors.joining("\n")));
        }
    }

    private List<List<String>> commandLines(String podName, String containerName, String cmd) {
        List<List<String>> result = new ArrayList<>();
        String output = cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "for pid in $(ps -C java -o pid h); do cat /proc/$pid/cmdline; done"
        ).out();
        for (String cmdLine : output.split("\n")) {
            result.add(asList(cmdLine.split("\0")));
        }
        return result;
    }

    protected void assertExpectedJavaOpts(String podName, String containerName, String expectedXmx, String expectedXms, String expectedServer, String expectedXx) {
        List<List<String>> cmdLines = commandLines(podName, containerName, "java");
        assertThat("Expected exactly 1 java process to be running", cmdLines.size(), is(1));
        List<String> cmd = cmdLines.get(0);
        int toIndex = cmd.indexOf("-jar");
        if (toIndex != -1) {
            // Just consider arguments to the JVM, not the application running in it
            cmd = cmd.subList(0, toIndex);
            // We should do something similar if the class not -jar was given, but that's
            // hard to do properly.
        }
        assertCmdOption(cmd, expectedXmx);
        assertCmdOption(cmd, expectedXms);
        assertCmdOption(cmd, expectedServer);
        assertCmdOption(cmd, expectedXx);
    }

    public Map<String, String> getImagesFromConfig() {
        Map<String, String> images = new HashMap<>();
        for (Container c : kubeClient().getDeployment("strimzi-cluster-operator").getSpec().getTemplate().getSpec().getContainers()) {
            for (EnvVar envVar : c.getEnv()) {
                images.put(envVar.getName(), envVar.getValue());
            }
        }
        return images;
    }

    /**
     * Wait till all pods in specific namespace being deleted and recreate testing environment in case of some pods cannot be deleted.
     * This method is {@Deprecated} and it should be removed in the future. Instead of use this method, you should use method {@link io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils#waitForPodDeletion(String)}
     * which should be used by default in deletion process for all components (it force pod deletion via executor instead of fabric8 client, which seems to be unstable).
     * @param time timeout in miliseconds
     * @throws Exception exception
     */
    @Deprecated
    void waitForDeletion(long time) throws Exception {
        List<Pod> pods = kubeClient().listPods().stream().filter(
            p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        // Delete pods in case of kubernetes keep them up
        pods.forEach(p -> kubeClient().deletePod(p));

        LOGGER.info("Wait for {} ms after cleanup to make sure everything is deleted", time);
        Thread.sleep(time);

        // Collect pods again after proper removal
        pods = kubeClient().listPods().stream().filter(
            p -> !p.getMetadata().getName().startsWith(CLUSTER_OPERATOR_PREFIX)).collect(Collectors.toList());
        long podCount = pods.size();

        StringBuilder nonTerminated = new StringBuilder();
        if (podCount > 0) {
            pods.forEach(
                p -> nonTerminated.append("\n").append(p.getMetadata().getName()).append(" - ").append(p.getStatus().getPhase())
            );
            throw new Exception("There are some unexpected pods! Cleanup is not finished properly!" + nonTerminated);
        }
    }

    /**
     * Deploy CO via helm chart. Using config file stored in test resources.
     */
    public void deployClusterOperatorViaHelmChart() {
        String dockerOrg = Environment.STRIMZI_ORG;
        String dockerTag = Environment.STRIMZI_TAG;

        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
            entry("imageRepositoryOverride", dockerOrg),
            entry("imageTagOverride", dockerTag),
            entry("image.pullPolicy", Environment.OPERATOR_IMAGE_PULL_POLICY),
            entry("resources.requests.memory", REQUESTS_MEMORY),
            entry("resources.requests.cpu", REQUESTS_CPU),
            entry("resources.limits.memory", LIMITS_MEMORY),
            entry("resources.limits.cpu", LIMITS_CPU),
            entry("logLevel", Environment.STRIMZI_LOG_LEVEL))
            .collect(TestUtils.entriesToMap()));

        LOGGER.info("Creating cluster operator with Helm Chart before test class {}", testClass);
        Path pathToChart = new File(HELM_CHART).toPath();
        String oldNamespace = cluster.setNamespace("kube-system");
        InputStream helmAccountAsStream = getClass().getClassLoader().getResourceAsStream("helm/helm-service-account.yaml");
        String helmServiceAccount = TestUtils.readResource(helmAccountAsStream);
        cmdKubeClient().applyContent(helmServiceAccount);
        helmClient().init();
        cluster.setNamespace(oldNamespace);
        helmClient().install(pathToChart, HELM_RELEASE_NAME, values);
    }

    /**
     * Delete CO deployed via helm chart.
     */
    public void deleteClusterOperatorViaHelmChart() {
        LOGGER.info("Deleting cluster operator with Helm Chart after test class {}", testClass);
        helmClient().delete(HELM_RELEASE_NAME);
    }


    /**
     * Verifies container configuration for specific component (kafka/zookeeper/bridge/mm) by environment key.
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param configKey Expected configuration key
     * @param config Expected component configuration
     */
    protected void checkComponentConfiguration(String podNamePrefix, String containerName, String configKey, Map<String, Object> config) {
        LOGGER.info("Getting pods by prefix in name {}", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing configuration for container {}", containerName);

            Map<String, Object> actual = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream()) // get containers
                .filter(c -> c.getName().equals(containerName))
                .flatMap(c -> c.getEnv().stream().filter(envVar -> envVar.getName().equals(configKey)))
                .map(envVar -> StUtils.loadProperties(envVar.getValue()))
                .collect(Collectors.toList()).get(0);

            assertThat(actual.entrySet().containsAll(config.entrySet()), is(true));
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies container environment variables passed as a map.
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param config Expected environment variables with values
     */
    protected void checkSpecificVariablesInContainer(String podNamePrefix, String containerName, Map<String, String> config) {
        LOGGER.info("Getting pods by prefix in name {}", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing EnvVars configuration for container {}", containerName);

            Map<String, Object> actual = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream()) // get containers
                .filter(c -> c.getName().equals(containerName))
                .flatMap(c -> c.getEnv().stream().filter(envVar -> config.containsKey(envVar.getName())))
                .collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue, (item, duplicatedItem) -> item));
            assertThat(actual, is(config));
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies readinessProbe and livenessProbe properties in expected container
     * @param podNamePrefix Prefix of pod name where container is located
     * @param containerName The container where verifying is expected
     * @param initialDelaySeconds expected value for property initialDelaySeconds
     * @param timeoutSeconds expected value for property timeoutSeconds
     * @param periodSeconds expected value for property periodSeconds
     * @param successThreshold expected value for property successThreshold
     * @param failureThreshold expected value for property failureThreshold
     */
    protected void checkReadinessLivenessProbe(String podNamePrefix, String containerName, int initialDelaySeconds, int timeoutSeconds,
                                               int periodSeconds, int successThreshold, int failureThreshold) {
        LOGGER.info("Getting pods by prefix {} in pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing Readiness and Liveness configuration for container {}", containerName);

            List<Container> containerList = pods.stream()
                .flatMap(p -> p.getSpec().getContainers().stream())
                .filter(c -> c.getName().equals(containerName))
                .collect(Collectors.toList());

            containerList.forEach(container -> {
                assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(initialDelaySeconds));
                assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(timeoutSeconds));
                assertThat(container.getLivenessProbe().getPeriodSeconds(), is(periodSeconds));
                assertThat(container.getReadinessProbe().getPeriodSeconds(), is(periodSeconds));
                assertThat(container.getLivenessProbe().getSuccessThreshold(), is(successThreshold));
                assertThat(container.getReadinessProbe().getSuccessThreshold(), is(successThreshold));
                assertThat(container.getLivenessProbe().getFailureThreshold(), is(failureThreshold));
                assertThat(container.getReadinessProbe().getFailureThreshold(), is(failureThreshold));
            });
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    void verifyLabelsForKafkaCluster(String clusterName, String appName) {
        verifyLabelsForKafkaOrZkPods(clusterName, "zookeeper", appName);
        verifyLabelsForKafkaOrZkPods(clusterName, "kafka", appName);
        verifyLabelsOnCOPod();
        verifyLabelsOnPods(clusterName, "entity-operator", appName, "Kafka");
        verifyLabelsForCRDs();
        verifyLabelsForKafkaAndZKServices(clusterName, appName);
        verifyLabelsForSecrets(clusterName, appName);
        verifyLabelsForConfigMaps(clusterName, appName, "");
        verifyLabelsForRoleBindings(clusterName, appName);
        verifyLabelsForServiceAccounts(clusterName, appName);
    }

    void verifyLabelsForKafkaOrZkPods(String clusterName, String podType, String appName) {
        LOGGER.info("Verifying labels for {}", podType);

        kubeClient().listPodsByPrefixInName(clusterName + "-" + podType).forEach(
            pod -> {
                String podName = pod.getMetadata().getName();
                LOGGER.info("Verifying labels for pod {}", podName);
                Map<String, String> labels = pod.getMetadata().getLabels();
                assertThat(labels.get("app"), is(appName));
                assertThat(labels.get("controller-revision-hash").matches("openshift-my-cluster-" + podType + "-.+"), is(true));
                assertThat(labels.get("statefulset.kubernetes.io/pod-name"), is(podName));
                assertThat(labels.get("strimzi.io/cluster"), is(clusterName));
                assertThat(labels.get("strimzi.io/kind"), is("Kafka"));
                assertThat(labels.get("strimzi.io/name"), is(clusterName.concat("-").concat(podType)));
            }
        );
    }

    void verifyLabelsOnCOPod() {
        LOGGER.info("Verifying labels for cluster-operator pod");

        Map<String, String> coLabels = kubeClient().listPods("name", "strimzi-cluster-operator").get(0).getMetadata().getLabels();
        assertThat(coLabels.get("name"), is("strimzi-cluster-operator"));
        assertThat(coLabels.get("strimzi.io/kind"), is("cluster-operator"));
    }

    protected void verifyLabelsOnPods(String clusterName, String podType, String appName, String kind) {
        LOGGER.info("Verifying labels on pod type {}", podType);
        kubeClient().listPods().stream()
            .filter(pod -> pod.getMetadata().getName().startsWith(clusterName.concat("-" + podType)))
            .forEach(pod -> {
                LOGGER.info("Verifying labels for pod: " + pod.getMetadata().getName());
                assertThat(pod.getMetadata().getLabels().get("app"), is(appName));
                assertThat(pod.getMetadata().getLabels().get("pod-template-hash").matches("[0-9A-Fa-f]+"), is(true));
                assertThat(pod.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
                assertThat(pod.getMetadata().getLabels().get("strimzi.io/kind"), is(kind));
                assertThat(pod.getMetadata().getLabels().get("strimzi.io/name"), is(clusterName.concat("-" + podType)));
            });
    }

    void verifyLabelsForCRDs() {
        LOGGER.info("Verifying labels for CRDs");
        kubeClient().listCustomResourceDefinition().stream()
            .filter(crd -> crd.getMetadata().getName().startsWith("kafka"))
            .forEach(crd -> {
                LOGGER.info("Verifying labels for custom resource {]", crd.getMetadata().getName());
                assertThat(crd.getMetadata().getLabels().get("app"), is("strimzi"));
            });
    }

    void verifyLabelsForKafkaAndZKServices(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Services");
        List<String> servicesList = new ArrayList<>();
        servicesList.add(clusterName + "-kafka-bootstrap");
        servicesList.add(clusterName + "-kafka-brokers");
        servicesList.add(clusterName + "-zookeeper-nodes");
        servicesList.add(clusterName + "-zookeeper-client");

        for (String serviceName : servicesList) {
            kubeClient().listServices().stream()
                .filter(service -> service.getMetadata().getName().equals(serviceName))
                .forEach(service -> {
                    LOGGER.info("Verifying labels for service {}", serviceName);
                    assertThat(service.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(service.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
                    assertThat(service.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                    assertThat(service.getMetadata().getLabels().get("strimzi.io/name"), is(serviceName));
                });
        }
    }

    protected void verifyLabelsForService(String clusterName, String serviceToTest, String kind) {
        LOGGER.info("Verifying labels for Kafka Connect Services");

        String serviceName = clusterName.concat("-").concat(serviceToTest);
        kubeClient().listServices().stream()
            .filter(service -> service.getMetadata().getName().equals(serviceName))
            .forEach(service -> {
                LOGGER.info("Verifying labels for service {}", service.getMetadata().getName());
                assertThat(service.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
                assertThat(service.getMetadata().getLabels().get("strimzi.io/kind"), is(kind));
                assertThat(service.getMetadata().getLabels().get("strimzi.io/name"), is(serviceName));
            }
        );
    }

    void verifyLabelsForSecrets(String clusterName, String appName) {
        LOGGER.info("Verifying labels for secrets");
        kubeClient().listSecrets().stream()
            .filter(p -> p.getMetadata().getName().matches("(" + clusterName + ")-(clients|cluster|(entity))(-operator)?(-ca)?(-certs?)?"))
            .forEach(p -> {
                LOGGER.info("Verifying secret {}", p.getMetadata().getName());
                assertThat(p.getMetadata().getLabels().get("app"), is(appName));
                assertThat(p.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                assertThat(p.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
            }
        );
    }

    void verifyLabelsForConfigMaps(String clusterName, String appName, String additionalClusterName) {
        LOGGER.info("Verifying labels for Config maps");

        kubeClient().listConfigMaps()
            .forEach(cm -> {
                LOGGER.info("Verifying labels for CM {}", cm.getMetadata().getName());
                if (cm.getMetadata().getName().equals(clusterName.concat("-connect-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/kind"), is("KafkaConnect"));
                } else if (cm.getMetadata().getName().contains("-mirror-maker-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/kind"), is("KafkaMirrorMaker"));
                } else if (cm.getMetadata().getName().contains("-mirrormaker2-config")) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/kind"), is("KafkaMirrorMaker2"));
                } else if (cm.getMetadata().getName().equals(clusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
                } else if (cm.getMetadata().getName().equals(additionalClusterName.concat("-kafka-config"))) {
                    assertThat(cm.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                    assertThat(cm.getMetadata().getLabels().get("strimzi.io/cluster"), is(additionalClusterName));
                } else {
                    LOGGER.info("CM {} is not related to current test", cm.getMetadata().getName());
                }
            }
        );
    }

    void verifyLabelsForServiceAccounts(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Service Accounts");

        kubeClient().listServiceAccounts().stream()
            .filter(sa -> sa.getMetadata().getName().equals("strimzi-cluster-operator"))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                assertThat(sa.getMetadata().getLabels().get("app"), is("strimzi"));
            }
        );

        kubeClient().listServiceAccounts().stream()
            .filter(sa -> sa.getMetadata().getName().startsWith(clusterName))
            .forEach(sa -> {
                LOGGER.info("Verifying labels for service account {}", sa.getMetadata().getName());
                if (sa.getMetadata().getName().equals(clusterName.concat("-connect"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get("strimzi.io/kind"), is("KafkaConnect"));
                } else if (sa.getMetadata().getName().equals(clusterName.concat("-mirror-maker"))) {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(nullValue()));
                    assertThat(sa.getMetadata().getLabels().get("strimzi.io/kind"), is("KafkaMirrorMaker"));
                } else {
                    assertThat(sa.getMetadata().getLabels().get("app"), is(appName));
                    assertThat(sa.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                }
                assertThat(sa.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
            }
        );
    }

    void verifyLabelsForRoleBindings(String clusterName, String appName) {
        LOGGER.info("Verifying labels for Cluster Role bindings");
        kubeClient().listRoleBindings().stream()
            .filter(rb -> rb.getMetadata().getName().startsWith("strimzi-cluster-operator"))
            .forEach(rb -> {
                LOGGER.info("Verifying labels for cluster role {}", rb.getMetadata().getName());
                assertThat(rb.getMetadata().getLabels().get("app"), is("strimzi"));
            });

        kubeClient().listRoleBindings().stream()
            .filter(rb -> rb.getMetadata().getName().startsWith("strimzi-".concat(clusterName)))
            .forEach(rb -> {
                LOGGER.info("Verifying labels for cluster role {}", rb.getMetadata().getName());
                assertThat(rb.getMetadata().getLabels().get("app"), is(appName));
                assertThat(rb.getMetadata().getLabels().get("strimzi.io/cluster"), is(clusterName));
                assertThat(rb.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
            }
        );
    }

    protected void verifyCRStatusCondition(Condition condition, String status, String type) {
        verifyCRStatusCondition(condition, null, null, status, type);
    }

    protected void verifyCRStatusCondition(Condition condition, String message, String reason, String status, String type) {
        assertThat(condition.getStatus(), is(status));
        assertThat(condition.getType(), is(type));

        if (condition.getMessage() != null && condition.getReason() != null) {
            assertThat(condition.getMessage(), containsString(message));
            assertThat(condition.getReason(), is(reason));
        }
    }

    protected void assertNoCoErrorsLogged(long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} seconds", sinceSeconds);
        String clusterOperatorLog = cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", sinceSeconds, "Exception", "Error", "Throwable");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    protected void tearDownEnvironmentAfterEach() throws Exception {
        ResourceManager.deleteMethodResources();
    }

    protected void tearDownEnvironmentAfterAll() {
        ResourceManager.deleteClassResources();
    }

    protected void testDockerImagesForKafkaCluster(String clusterName, String namespace, int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig();

        String kafkaVersion = Crds.kafkaOperation(kubeClient().getClient()).inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getVersion();
        if (kafkaVersion == null) {
            kafkaVersion = Environment.ST_KAFKA_VERSION;
        }

        //Verifying docker image for zookeeper pods
        for (int i = 0; i < zkPods; i++) {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.zookeeperPodName(clusterName, i), "zookeeper");
            assertThat("Zookeeper pod " + i + " uses wrong image", TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion), is(imgFromPod));
            imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.zookeeperPodName(clusterName, i), "tls-sidecar");
            assertThat("Zookeeper TLS side car for pod " + i + " uses wrong image", imgFromDeplConf.get(TLS_SIDECAR_ZOOKEEPER_IMAGE), is(imgFromPod));
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.kafkaPodName(clusterName, i), "kafka");
            assertThat("Kafka pod " + i + " uses wrong image", TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion), is(imgFromPod));
            imgFromPod = PodUtils.getContainerImageNameFromPod(KafkaResources.kafkaPodName(clusterName, i), "tls-sidecar");
            assertThat("Kafka TLS side car for pod " + i + " uses wrong image", imgFromDeplConf.get(TLS_SIDECAR_KAFKA_IMAGE), is(imgFromPod));
            if (rackAwareEnabled) {
                String initContainerImage = PodUtils.getInitContainerImageName(KafkaResources.kafkaPodName(clusterName, i));
                assertThat(initContainerImage, is(imgFromDeplConf.get(KAFKA_INIT_IMAGE)));
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod",
                "strimzi.io/name=" + clusterName + "-entity-operator").get(0);
        String imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "topic-operator");
        assertThat(imgFromPod, is(imgFromDeplConf.get(TO_IMAGE)));
        imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "user-operator");
        assertThat(imgFromPod, is(imgFromDeplConf.get(UO_IMAGE)));
        imgFromPod = PodUtils.getContainerImageNameFromPod(entityOperatorPodName, "tls-sidecar");
        assertThat(imgFromPod, is(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE)));

        LOGGER.info("Docker images verified");
    }

    @BeforeEach
    void createTestResources(TestInfo testInfo) {
        if (testInfo.getTestMethod().isPresent()) {
            testName = testInfo.getTestMethod().get().getName();
        }
        ResourceManager.setMethodResources();
    }

    @BeforeAll
    void setTestClassName(TestInfo testInfo) {
        if (testInfo.getTestClass().isPresent()) {
            testClass = testInfo.getTestClass().get().getName();
        }
    }

    @AfterEach
    void teardownEnvironmentMethod(ExtensionContext context) throws Exception {
        boolean logError = false;
        AssertionError assertionError = new AssertionError();
        try {
            assertNoCoErrorsLogged(0);
        } catch (AssertionError e) {
            LOGGER.error("Cluster Operator contains unexpected errors!");
            logError = true;
            assertionError = e;
        }

        if (Environment.SKIP_TEARDOWN == null) {
            if (context.getExecutionException().isPresent() || logError) {
                LOGGER.info("Test execution contains exception, going to recreate test environment");
                recreateTestEnv(cluster.getTestNamespace(), cluster.getBindingsNamespaces());
                LOGGER.info("Env recreated.");
            }
            tearDownEnvironmentAfterEach();
        }

        if (logError) {
            throw assertionError;
        }
    }

    @AfterAll
    void teardownEnvironmentClass() {
        if (Environment.SKIP_TEARDOWN == null) {
            tearDownEnvironmentAfterAll();
            teardownEnvForOperator();
        }
    }
}
