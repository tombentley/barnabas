/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.KubernetesResourceMappingProvider;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceSubresourceStatus;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.crdgenerator.annotations.Crd;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class StrimziResourceMappingProvider implements KubernetesResourceMappingProvider, CrdProvider {

    static class CrdDescriptor {
        public final String group;
        public final String crdApiVersion;
        public final String version;
        public final String kind;
        public final Class<? extends CustomResource> modelClass;
        public final String plural;
        public final String singular;
        public final String listKind;
        public final String scope;
        public final Class<? extends CustomResourceSubresourceStatus> statusClass;

        @JsonCreator
        public CrdDescriptor(@JsonProperty("group") String group,
                             @JsonProperty("crdApiVersion") String crdApiVersion,
                             @JsonProperty("version") String version,
                             @JsonProperty("kind") String kind,
                             @JsonProperty("modelClass") Class<? extends CustomResource> modelClass,
                             @JsonProperty("plural") String plural,
                             @JsonProperty("singular") String singular,
                             @JsonProperty("listKind") String listKind,
                             @JsonProperty("scope") String scope,
                             @JsonProperty("statusClass") Class<? extends CustomResourceSubresourceStatus> statusClass) {
            this.group = group;
            this.crdApiVersion = crdApiVersion;
            this.version = version;
            this.kind = kind;
            this.modelClass = modelClass;
            this.plural = plural;
            this.singular = singular;
            this.listKind = listKind;
            this.scope = scope;
            this.statusClass = statusClass;
        }
    }

    private final Map<String, Class<? extends KubernetesResource>> mappings;
    private Map<String, CustomResourceDefinition> crds = new HashMap<>();

    @SuppressWarnings("unchecked")
    public StrimziResourceMappingProvider() throws IllegalAccessException, InstantiationException {

        String resourceName = "/strimzi-crds.yaml";
        YAMLMapper mapper = new YAMLMapper();
        List<CrdDescriptor> l;
        try (InputStream is = getClass().getResourceAsStream(resourceName)) {
            l = mapper.readValue(is, new TypeReference<List<CrdDescriptor>>() { });
        } catch (IOException e) {
            throw new RuntimeException("Could not load classpath resource " + resourceName + " or it was not a YAML file", e);
        }
        this.mappings = new HashMap<>();
        for (CrdDescriptor descriptor : l) {
            String key = descriptor.group + "/" + descriptor.version + "#" + descriptor.kind;
            if (KubernetesResource.class.isAssignableFrom(descriptor.modelClass)) {
                mappings.put(key, descriptor.modelClass);
            } else {
                throw new RuntimeException(descriptor.modelClass + " is not assignable from KubernetesResource");
            }
            crds.put(key,  new CustomResourceDefinitionBuilder()
                    .withApiVersion(descriptor.crdApiVersion)
                    .withKind(descriptor.kind)
                    .withNewMetadata()
                    .withName(descriptor.plural + "." + descriptor.group)
                    .endMetadata()
                    .withNewSpec()
                    .withScope(descriptor.scope)
                    .withGroup(descriptor.group)
                    .withVersion(descriptor.version)
                    .withNewNames()
                    .withSingular(descriptor.singular)
                    .withPlural(descriptor.plural)
                    .withKind(descriptor.kind)
                    .withListKind(descriptor.listKind)
                    .endNames()
                    .withNewSubresources()
                    .withStatus(descriptor.statusClass.newInstance())
                    .endSubresources()
                    .endSpec()
                    .build());

        }
    }

    @Override
    public Map<String, Class<? extends KubernetesResource>> getMappings() {
        return mappings;
    }

    public Map<String, CustomResourceDefinition> crds() {
        return crds;
    }

    public <T extends KubernetesResource&HasMetadata,
            L extends KubernetesResourceList<T>,
            D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> operation(KubernetesClient client,
                                                                                     Class<T> cls,
                                                                                     Class<L> listCls,
                                                                                     Class<D> doneableCls) {
        // TODO This doesn't work because the annotations jar is not on the runtime classpath.
        Crd annotation = cls.getAnnotation(Crd.class);
        String key = annotation.spec().group() + "/" +
                annotation.spec().version() + "#" +
                annotation.spec().names().kind();
        return client.customResources(
                crds.get(key),
                cls,
                listCls,
                doneableCls);
    }

    public static void main(String[] args) {
        for (KubernetesResourceMappingProvider provider : ServiceLoader.load(KubernetesResourceMappingProvider.class)) {
            for (Map.Entry<String, Class<? extends KubernetesResource>> entry : provider.getMappings().entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }

        for (CrdProvider provider : ServiceLoader.load(CrdProvider.class)) {
            for (Map.Entry<String, CustomResourceDefinition> entry : provider.crds().entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
    }
}
