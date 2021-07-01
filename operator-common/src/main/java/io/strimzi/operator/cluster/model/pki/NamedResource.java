/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.dsl.CreateOrReplaceable;
import io.fabric8.kubernetes.client.dsl.Createable;
import io.fabric8.kubernetes.client.dsl.Gettable;
import io.fabric8.kubernetes.client.dsl.Lockable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Replaceable;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Like {@link Resource}, only knows it's name so that:
 * <ul>
 * <li>You can call create with an instances without a {@code metadata.name} set.</li>
 * <li>It can be used in logging</li>
 * </ul>
 * This means that clients don't have to directly embed knowledge of the name of the Secret used.
 * @param <T> The type of Kubernetes resource
 */
public class NamedResource<T extends HasMetadata>
        implements Gettable<T>,
        Createable<T>,
        CreateOrReplaceable<T>,
        Lockable<NamedResource<T>>,
        Replaceable<T> {

    private final String name;

    private final Resource<T> resource;

    public NamedResource(NonNamespaceOperation<T, ? extends KubernetesResourceList<T>, ? extends Resource<T>> op, String name) {
        this(name, op.withName(name));
    }

    public NamedResource(String name, Resource<T> resource) {
        this.name = name;
        this.resource = resource;
    }


    public T get() {
        return resource.get();
    }

    public T create(T item) {
        maybeSetName(item);
        return resource.create(item);
    }

    @SafeVarargs
    public final T create(T... items) {
        maybeSetNames(items);
        return resource.create(items);
    }

    @SafeVarargs
    private void maybeSetNames(T... items) {
        for (T item : items) {
            maybeSetName(item);
        }
    }

    private void maybeSetName(T item) {
        ObjectMeta meta = item.getMetadata();
        if (meta == null) {
            meta = new ObjectMeta();
            item.setMetadata(meta);
        }
        if (meta.getName() == null) {
            if (meta.getGenerateName() != null) {
                throw new IllegalArgumentException();
            }
            meta.setName(name);
        } else if (!meta.getName().equals(name)) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return "RefImpl(" +
                "name='" + name + '\'' +
                ')';
    }

//    public static void main(String[] a) {
//        KubernetesClient client = new DefaultKubernetesClient();
//        NonNamespaceOperation<Secret, SecretList, Resource<Secret>> myproject = client.secrets().inNamespace("myproject");
//        NamedResource<Secret> secretResource = new NamedResource<>(myproject, "mysecret");
//        System.out.println(secretResource);
//        secretResource.create(new SecretBuilder().addToData("foo", Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.US_ASCII))).build());
//    }

    @SafeVarargs
    @Override
    public final T createOrReplace(T... items) {
        maybeSetNames(items);
        return this.resource.createOrReplace(items);

    }

    public NamedResource<T> lockResourceVersion(String resourceVersion) {
        return new NamedResource<T>(name, (Resource<T>) resource.lockResourceVersion(resourceVersion));
    }

    @Override
    public T replace(T item) {
        maybeSetName(item);
        return resource.replace(item);
    }
}
