/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceSubresourceStatus;
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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {

    public static final String CRD_KIND = "CustomResourceDefinition";
    public static final String STRIMZI_CATEGORY = "strimzi";

    private static final Map<String, CustomResourceDefinition> CRDS = new HashMap<>();
    static {
        for (CrdProvider provider : ServiceLoader.load(CrdProvider.class)) {
            CRDS.putAll(provider.crds());
        }
    }

    private Crds() {
    }

    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls) {
        String version = null;
        if (cls.equals(Kafka.class)) {
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaConnect.class)) {
            version = KafkaConnect.VERSIONS.get(0);
        } else if (cls.equals(KafkaConnectS2I.class)) {
            version = KafkaConnectS2I.VERSIONS.get(0);
        } else if (cls.equals(KafkaTopic.class)) {
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaUser.class)) {
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            version = KafkaMirrorMaker.VERSIONS.get(0);
        } else if (cls.equals(KafkaBridge.class)) {
            version = KafkaBridge.VERSIONS.get(0);
        } else if (cls.equals(KafkaConnector.class)) {
            version = KafkaConnector.VERSIONS.get(0);
        } else if (cls.equals(KafkaMirrorMaker2.class)) {
            version = KafkaMirrorMaker2.VERSIONS.get(0);
        } else {
            throw new RuntimeException();
        }

        return crd(cls, version);
    }

    @SuppressWarnings("checkstyle:JavaNCSS")
    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls, String version) {
        String scope, crdApiVersion, plural, singular, group, kind, listKind;
        CustomResourceSubresourceStatus status = null;

        if (cls.equals(Kafka.class)) {
            scope = Kafka.SCOPE;
            crdApiVersion = Kafka.CRD_API_VERSION;
            plural = Kafka.RESOURCE_PLURAL;
            singular = Kafka.RESOURCE_SINGULAR;
            group = Kafka.RESOURCE_GROUP;
            kind = Kafka.RESOURCE_KIND;
            listKind = Kafka.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!Kafka.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnect.class)) {
            scope = KafkaConnect.SCOPE;
            crdApiVersion = KafkaConnect.CRD_API_VERSION;
            plural = KafkaConnect.RESOURCE_PLURAL;
            singular = KafkaConnect.RESOURCE_SINGULAR;
            group = KafkaConnect.RESOURCE_GROUP;
            kind = KafkaConnect.RESOURCE_KIND;
            listKind = KafkaConnect.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnect.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnectS2I.class)) {
            scope = KafkaConnectS2I.SCOPE;
            crdApiVersion = KafkaConnectS2I.CRD_API_VERSION;
            plural = KafkaConnectS2I.RESOURCE_PLURAL;
            singular = KafkaConnectS2I.RESOURCE_SINGULAR;
            group = KafkaConnectS2I.RESOURCE_GROUP;
            kind = KafkaConnectS2I.RESOURCE_KIND;
            listKind = KafkaConnectS2I.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnectS2I.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaTopic.class)) {
            scope = KafkaTopic.SCOPE;
            crdApiVersion = KafkaTopic.CRD_API_VERSION;
            plural = KafkaTopic.RESOURCE_PLURAL;
            singular = KafkaTopic.RESOURCE_SINGULAR;
            group = KafkaTopic.RESOURCE_GROUP;
            kind = KafkaTopic.RESOURCE_KIND;
            listKind = KafkaTopic.RESOURCE_LIST_KIND;
            if (!KafkaTopic.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaUser.class)) {
            scope = KafkaUser.SCOPE;
            crdApiVersion = KafkaUser.CRD_API_VERSION;
            plural = KafkaUser.RESOURCE_PLURAL;
            singular = KafkaUser.RESOURCE_SINGULAR;
            group = KafkaUser.RESOURCE_GROUP;
            kind = KafkaUser.RESOURCE_KIND;
            listKind = KafkaUser.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaUser.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            scope = KafkaMirrorMaker.SCOPE;
            crdApiVersion = KafkaMirrorMaker.CRD_API_VERSION;
            plural = KafkaMirrorMaker.RESOURCE_PLURAL;
            singular = KafkaMirrorMaker.RESOURCE_SINGULAR;
            group = KafkaMirrorMaker.RESOURCE_GROUP;
            kind = KafkaMirrorMaker.RESOURCE_KIND;
            listKind = KafkaMirrorMaker.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaMirrorMaker.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaBridge.class)) {
            scope = KafkaBridge.SCOPE;
            crdApiVersion = KafkaBridge.CRD_API_VERSION;
            plural = KafkaBridge.RESOURCE_PLURAL;
            singular = KafkaBridge.RESOURCE_SINGULAR;
            group = KafkaBridge.RESOURCE_GROUP;
            kind = KafkaBridge.RESOURCE_KIND;
            listKind = KafkaBridge.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaBridge.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnector.class)) {
            scope = KafkaConnector.SCOPE;
            crdApiVersion = KafkaConnector.CRD_API_VERSION;
            plural = KafkaConnector.RESOURCE_PLURAL;
            singular = KafkaConnector.RESOURCE_SINGULAR;
            group = KafkaConnector.RESOURCE_GROUP;
            kind = KafkaConnector.RESOURCE_KIND;
            listKind = KafkaConnector.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnector.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaMirrorMaker2.class)) {
            scope = KafkaMirrorMaker2.SCOPE;
            crdApiVersion = KafkaMirrorMaker2.CRD_API_VERSION;
            plural = KafkaMirrorMaker2.RESOURCE_PLURAL;
            singular = KafkaMirrorMaker2.RESOURCE_SINGULAR;
            group = KafkaMirrorMaker2.RESOURCE_GROUP;
            kind = KafkaMirrorMaker2.RESOURCE_KIND;
            listKind = KafkaMirrorMaker2.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaMirrorMaker2.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }                
        } else {
            throw new RuntimeException();
        }

        return new CustomResourceDefinitionBuilder()
                .withApiVersion(crdApiVersion)
                .withKind(CRD_KIND)
                .withNewMetadata()
                    .withName(plural + "." + group)
                .endMetadata()
                .withNewSpec()
                    .withScope(scope)
                    .withGroup(group)
                    .withVersion(version)
                    .withNewNames()
                        .withSingular(singular)
                        .withPlural(plural)
                        .withKind(kind)
                        .withListKind(listKind)
                    .endNames()
                    .withNewSubresources()
                        .withStatus(status)
                    .endSubresources()
                .endSpec()
                .build();
    }

    public static CustomResourceDefinition kafka() {
        return CRDS.get("kafka.strimzi.io/v1beta1#Kafka");
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaOperation(KubernetesClient client) {
        return client.customResources(kafka(), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaV1Alpha1Operation(KubernetesClient client) {
        return client.customResources(CRDS.get("kafka.strimzi.io/v1alpha1#Kafka"), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaConnect");
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);
    }

    public static CustomResourceDefinition kafkaConnector() {
        return CRDS.get("kafka.strimzi.io/v1alpha1#KafkaConnector");
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> kafkaConnectorOperation(KubernetesClient client) {
        return client.customResources(kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaConnectS2I");
    }

    public static <D extends CustomResourceDoneable<T>, T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class);
    }

    public static CustomResourceDefinition topic() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaTopic");
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> topicOperation(KubernetesClient client) {
        return client.customResources(topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    public static CustomResourceDefinition kafkaUser() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaUser");
    }

    public static MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUserOperation(KubernetesClient client) {
        return client.customResources(kafkaUser(), KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    public static CustomResourceDefinition mirrorMaker() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaMirrorMaker");
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client) {
        return client.customResources(mirrorMaker(), KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
    }

    public static CustomResourceDefinition kafkaBridge() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaBridge");
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, DoneableKafkaBridge, Resource<KafkaBridge, DoneableKafkaBridge>> kafkaBridgeOperation(KubernetesClient client) {
        return client.customResources(kafkaBridge(), KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class);
    }

    public static CustomResourceDefinition kafkaMirrorMaker2() {
        return CRDS.get("kafka.strimzi.io/v1beta1#KafkaMirrorMaker2");
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2, Resource<KafkaMirrorMaker2, DoneableKafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client) {
        return client.customResources(kafkaMirrorMaker2(), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>>
            operation(KubernetesClient client,
                      Class<T> cls,
                      Class<L> listCls,
                      Class<D> doneableCls) {
        return client.customResources(crd(cls), cls, listCls, doneableCls);
    }

    public static <T extends CustomResource> String kind(Class<T> cls) {
        try {
            return cls.newInstance().getKind();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
