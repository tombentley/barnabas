/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

import java.util.ServiceLoader;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {

    public static final String STRIMZI_CATEGORY = "strimzi";

    private static final CrdProvider provider = ServiceLoader.load(CrdProvider.class).iterator().next();

    private Crds() {
    }

    public static CustomResourceDefinition kafka() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#Kafka");
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaOperation(KubernetesClient client) {
        return client.customResources(kafka(), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaV1Alpha1Operation(KubernetesClient client) {
        return client.customResources(provider.crds().get("kafka.strimzi.io/v1alpha1#Kafka"), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaConnect");
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);
    }

    public static CustomResourceDefinition kafkaConnector() {
        return provider.crds().get("kafka.strimzi.io/v1alpha1#KafkaConnector");
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> kafkaConnectorOperation(KubernetesClient client) {
        return client.customResources(kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaConnectS2I");
    }

    public static <D extends CustomResourceDoneable<T>, T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class);
    }

    public static CustomResourceDefinition topic() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaTopic");
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> topicOperation(KubernetesClient client) {
        return client.customResources(topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    public static CustomResourceDefinition kafkaUser() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaUser");
    }

    public static MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUserOperation(KubernetesClient client) {
        return client.customResources(kafkaUser(), KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    public static CustomResourceDefinition mirrorMaker() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaMirrorMaker");
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client) {
        return client.customResources(mirrorMaker(), KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
    }

    public static CustomResourceDefinition kafkaBridge() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaBridge");
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, DoneableKafkaBridge, Resource<KafkaBridge, DoneableKafkaBridge>> kafkaBridgeOperation(KubernetesClient client) {
        return client.customResources(kafkaBridge(), KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class);
    }

    public static CustomResourceDefinition kafkaMirrorMaker2() {
        return provider.crds().get("kafka.strimzi.io/v1beta1#KafkaMirrorMaker2");
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2, Resource<KafkaMirrorMaker2, DoneableKafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client) {
        return client.customResources(kafkaMirrorMaker2(), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>>
            operation(KubernetesClient client,
                      Class<T> cls,
                      Class<L> listCls,
                      Class<D> doneableCls) {
        return provider.operation(client, cls, listCls, doneableCls);
    }

    public static <T extends CustomResource> String kind(Class<T> cls) {
        try {
            return cls.newInstance().getKind();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
