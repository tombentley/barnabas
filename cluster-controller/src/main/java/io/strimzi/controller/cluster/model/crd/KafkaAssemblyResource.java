/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListMeta;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class KafkaAssemblyResource implements KubernetesResource<KafkaAssembly>, KubernetesResourceList<KafkaAssembly> {

    @JsonProperty("apiVersion")
    public String getApiVersion() {
        return "";
    }
    @Override
    public ListMeta getMetadata() {
        return null;
    }

    @Override
    public List<KafkaAssembly> getItems() {
        return null;
    }
}
