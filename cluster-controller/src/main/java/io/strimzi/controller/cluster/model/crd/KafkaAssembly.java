/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class KafkaAssembly extends CustomResource implements HasMetadata {
    private String apiVersion;
    private ObjectMeta metadata;
    private Kafka kafka;
    private Zookeeper zookeeper;
    private TopicController topicController;

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @JsonIgnore
    @JsonProperty("kind")
    @Override
    public String getKind() {
        return "Kafka";
    }

    @Override
    public ObjectMeta getMetadata() {
        return metadata;
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Zookeeper getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(Zookeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public TopicController getTopicController() {
        return topicController;
    }

    public void setTopicController(TopicController topicController) {
        this.topicController = topicController;
    }

    @Override
    public String toString() {
        return "Kafka{" +
                "kind='" + getKind() + '\'' +
                ", apiVersion='" + apiVersion + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
