/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * An opaque reference to a "resource" stored in a system that can be represented as a string using a
 * URI-(more strictly URN)-based syntax.
 */
public abstract class Ref<T> {

    public static Ref<?> parse(String reference) {
        URI uri;
        try {
            uri = URI.create(reference);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Not a valid URI: " + e.getMessage());
        }

        if (uri.getScheme() == null ||
                "kubernetes".equals(uri.getScheme())) {
            return KubeRef.parse(uri);
        } else {
            throw new IllegalArgumentException("Unknown schema in ref: " + reference);
        }
    }

    public abstract <U> boolean is(Class<U> c);

    public abstract <U> Ref<U> as(Class<U> c);

    public abstract String fragment();

    static class Resolver {
        private final KubernetesClient client;

        public Resolver(KubernetesClient client) {
            this.client = client;
        }

        KubernetesClient client() {
            return client;
        }
    }

    public abstract T get(Resolver resolver);

    static class KubeRef<T extends HasMetadata> extends Ref<T> {

        private final String kind;
        private final Class<T> kindClass;
        private final String namespace;
        private final String name;
        private final String fragment;

        public KubeRef(String kind, String namespace, String name, String fragment) {
            this.kind = kind;
            this.kindClass = null;
            this.namespace = namespace;
            this.name = Objects.requireNonNull(name);
            this.fragment = fragment;
        }

        public KubeRef(Class<T> kind, String namespace, String name, String fragment) {
            this.kindClass = kind;
            this.kind = null;
            this.namespace = namespace;
            this.name = Objects.requireNonNull(name);
            this.fragment = fragment;
        }

        public String kind() {
            return kind;
        }

        public String namespace() {
            return namespace;
        }

        public String name() {
            return name;
        }

        @Override
        public String fragment() {
            return fragment;
        }

        private static KubeRef parse(URI uri) {
            String path = uri.getPath();
            if (path == null) {
                throw new IllegalArgumentException("kubernetes reference lacking path: " + uri);
            }
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            List<String> split = List.of(path.split("/"));
            if (split.size() > 3) {
                throw new IllegalArgumentException("kubernetes reference path has too many parts (only paths like 'name' or 'namespace/name' are supported): " + uri);
            }
            if (split.size() == 3) {
                return new KubeRef(split.get(0), split.get(1), split.get(2), uri.getFragment());
            } else if (split.size() == 2) {
                return new KubeRef((String) null, split.get(0), split.get(1), uri.getFragment());
            } else {
                return new KubeRef((String) null, null, split.get(0), uri.getFragment());
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (kind != null) {
                sb.append(kind).append('/');
            }
            if (namespace != null) {
                sb.append(namespace).append('/');
            }
            sb.append(name);
            if (fragment != null) {
                sb.append('#').append(fragment);
            }
            return sb.toString();
        }

        static Map<Class<? extends HasMetadata>, Function<KubernetesClient, ? extends MixedOperation<?, ?, ? extends Resource<?>>>> kindMap = Map.of(
                Secret.class, KubernetesClient::secrets,
                ConfigMap.class, KubernetesClient::configMaps);

        public <T extends HasMetadata> T resolve(KubernetesClient client, Class<T> kind, String namespace) {
            if (!isKindCompatibleWith(kind)) {
                throw new IllegalArgumentException("Mismatching kinds: " + this.kind + ", " + kind);
            }
            if (this.namespace != null && !this.namespace.equals(namespace)) {
                throw new IllegalArgumentException("Mismatching namespaces: " + this.namespace + ", " + namespace);
            }
            MixedOperation<?, ?, ?> apply = kindMap.get(kind).apply(client);
            NonNamespaceOperation<?, ?, ? extends Resource<?>> nonNamespaceOperation = apply.inNamespace(namespace);
            Object o = nonNamespaceOperation.withName(name).get();
            if (o == null) {
                return null;
            } else if (kind.isInstance(o)) {
                return kind.cast(o);
            } else {
                throw new RuntimeException("Get object of " + o.getClass() + " for reference " + this + " but resolved using object of class " + kind);
            }
        }

        private <T extends HasMetadata> boolean isKindCompatibleWith(Class<T> kind) {
            return this.kind == null || kind.getSimpleName().toLowerCase(Locale.ENGLISH).equals(this.kind);
        }

        @Override
        public <T> boolean is(Class<T> c) {
            return HasMetadata.class.isAssignableFrom(c) && isKindCompatibleWith(c.asSubclass(HasMetadata.class));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Ref<T> as(Class<T> c) {
            if (HasMetadata.class.isAssignableFrom(c)
                    && isKindCompatibleWith(c.asSubclass(HasMetadata.class))) {
                return new KubeRef(c, this.namespace, this.name, this.fragment);
            } else {
                throw new IllegalArgumentException("A Ref<" + this.kind + "> is not a Ref<" + c + ">");
            }
        }

        @Override
        public T get(Resolver resolver) {
            MixedOperation<?, ?, ?> apply = kindMap.get(kindClass).apply(resolver.client());
            NonNamespaceOperation<?, ?, ? extends Resource<?>> nonNamespaceOperation = apply.inNamespace(namespace);
            Object o = nonNamespaceOperation.withName(name).get();
            if (o == null) {
                return null;
            } else if (kindClass.isInstance(o)) {
                return kindClass.cast(o);
            } else {
                throw new RuntimeException("Get object of " + o.getClass() + " for reference " + this + " but resolved using object of class " + kind);
            }
        }
    }

    public static void main(String[] a) {
        DefaultKubernetesClient client = new DefaultKubernetesClient();
        Resolver resolver = new Resolver(client);
        for (String refStr : List.of(
                "kubernetes:/configmap/myproject/strimzi-cluster-operator#item",
                "kubernetes:/myproject/strimzi-cluster-operator#item",
                "kubernetes:/strimzi-cluster-operator#item",
                "/configmap/myproject/strimzi-cluster-operator#item",
                "/myproject/strimzi-cluster-operator#item",
                "/strimzi-cluster-operator#item",
                "configmap/myproject/strimzi-cluster-operator#item",
                "myproject/strimzi-cluster-operator#item",
                "strimzi-cluster-operator#item",
                "secret/myproject/my-cluster-clients-ca#ca.key")) {
            System.out.println(refStr);
            Ref<?> ref = Ref.parse(refStr);
            if (ref.is(ConfigMap.class)) {
                Ref<ConfigMap> cmRef = ref.as(ConfigMap.class);
                System.out.println(cmRef.get(resolver).getData().keySet());
            } else if (ref.is(Secret.class)) {
                Ref<Secret> cmRef = ref.as(Secret.class);
                System.out.println(new String(Base64.getDecoder().decode(cmRef.get(resolver).getData().get(ref.fragment()))));
            }

        }
    }
}
