/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;

import java.io.File;
import java.util.Map;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class Kafka extends AbstractSsLike {

    public Kafka() {
        this.image = "strimzi/kafka:latest";
    }

    private Map<String, String> config;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
/*
    public static void main(String[] args) throws Exception {
        YAMLMapper mapper = new YAMLMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        KubernetesDeserializer.registerCustomKind("Kafka", KafkaAssembly.class);
        //SimpleModule module = new SimpleModule();
        //module.addSerializer(HasMetadata.class, new KubernetesDeserializer());
        //mapper.registerModule(module);

        File s = new File("strimzi-ephemeral.json");
        System.out.println(s);

        System.out.println(mapper.readValue(s, KafkaAssembly.class).getKafka().getResources().getLimits().get("memory"));
    }
    */

    public static void main(String[] args) throws Exception {
        KubernetesClient client = new DefaultKubernetesClient();
        CustomResourceDefinition crd = client.customResourceDefinitions().withName("kafkas.cluster-controller.strimzi.io").get();
        MixedOperation<KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly, Resource<KafkaAssembly, DoneableKafkaAssembly>> myClient = client.customResources(crd, KafkaAssembly.class, KafkaAssemblyList.class, DoneableKafkaAssembly.class);
        for (KafkaAssembly ka : myClient.list().getItems()) {
            System.out.println(ka.getKafka().getJavaOptions().getXms());
            System.out.println(ka.getKafka().getJavaOptions().getXmx());
        }

    }
}
