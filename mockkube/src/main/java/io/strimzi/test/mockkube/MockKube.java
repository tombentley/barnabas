/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableEndpoints;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.DoneableServiceAccount;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.networking.DoneableNetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyList;
import io.fabric8.kubernetes.api.model.policy.DoneablePodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetList;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.CreateOrReplaceable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.openshift.api.model.DoneableRoute;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockKube {

    private final Map<String, ConfigMap> cmDb = db(emptySet(), ConfigMap.class, DoneableConfigMap.class);
    private final Map<String, PersistentVolumeClaim> pvcDb = db(emptySet(), PersistentVolumeClaim.class, DoneablePersistentVolumeClaim.class);
    private final Map<String, Service> svcDb = db(emptySet(), Service.class, DoneableService.class);
    private final Map<String, Endpoints> endpointDb = db(emptySet(), Endpoints.class, DoneableEndpoints.class);
    private final Map<String, Pod> podDb = db(emptySet(), Pod.class, DoneablePod.class);
    private final Map<String, StatefulSet> ssDb = db(emptySet(), StatefulSet.class, DoneableStatefulSet.class);
    private final Map<String, Deployment> depDb = db(emptySet(), Deployment.class, DoneableDeployment.class);
    private final Map<String, Secret> secretDb = db(emptySet(), Secret.class, DoneableSecret.class);
    private final Map<String, ServiceAccount> serviceAccountDb = db(emptySet(), ServiceAccount.class, DoneableServiceAccount.class);
    private final Map<String, NetworkPolicy> policyDb = db(emptySet(), NetworkPolicy.class, DoneableNetworkPolicy.class);
    private final Map<String, Route> routeDb = db(emptySet(), Route.class, DoneableRoute.class);
    private final Map<String, PodDisruptionBudget> pdbDb = db(emptySet(), PodDisruptionBudget.class, DoneablePodDisruptionBudget.class);
    private final Map<String, RoleBinding> pdbRb = db(emptySet(), RoleBinding.class,
            DoneableRoleBinding.class);
    private final Map<String, ClusterRoleBinding> pdbCrb = db(emptySet(), ClusterRoleBinding.class,
            DoneableClusterRoleBinding.class);


    private Map<String, CreateOrReplaceable> crdMixedOps = new HashMap<>();

    public MockKube withInitialCms(Set<ConfigMap> initialCms) {
        this.cmDb.putAll(db(initialCms, ConfigMap.class, DoneableConfigMap.class));
        return this;
    }

    public MockKube withInitialStatefulSets(Set<StatefulSet> initial) {
        this.ssDb.putAll(db(initial, StatefulSet.class, DoneableStatefulSet.class));
        return this;
    }

    public MockKube withInitialPods(Set<Pod> initial) {
        this.podDb.putAll(db(initial, Pod.class, DoneablePod.class));
        return this;
    }

    public MockKube withInitialSecrets(Set<Secret> initial) {
        this.secretDb.putAll(db(initial, Secret.class, DoneableSecret.class));
        return this;
    }

    public MockKube withInitialNetworkPolicy(Set<NetworkPolicy> initial) {
        this.policyDb.putAll(db(initial, NetworkPolicy.class, DoneableNetworkPolicy.class));
        return this;
    }

    public MockKube withInitialRoute(Set<Route> initial) {
        this.routeDb.putAll(db(initial, Route.class, DoneableRoute.class));
        return this;
    }

    private final List<MockedCrd> mockedCrds = new ArrayList<>();

    public class MockedCrd<T extends CustomResource, L extends KubernetesResourceList<T>, D extends Doneable<T>> {
        private final CustomResourceDefinition crd;
        private final Class<T> crClass;
        private final Class<L> crListClass;
        private final Class<D> crDoneableClass;
        private final Map<String, T> instances;

        private MockedCrd(CustomResourceDefinition crd, Class<T> crClass, Class<L> crListClass, Class<D> crDoneableClass) {
            this.crd = crd;
            this.crClass = crClass;
            this.crListClass = crListClass;
            this.crDoneableClass = crDoneableClass;
            instances = db(emptySet(), crClass, crDoneableClass);
        }

        Map<String, T> getInstances() {
            return instances;
        }

        CustomResourceDefinition getCrd() {
            return crd;
        }

        Class<T> getCrClass() {
            return crClass;
        }

        Class<L> getCrListClass() {
            return crListClass;
        }

        Class<D> getCrDoneableClass() {
            return crDoneableClass;
        }

        public MockedCrd<T, L, D> withInitialInstances(Set<T> instances) {
            for (T instance : instances) {
                this.instances.put(instance.getMetadata().getName(), instance);
            }
            return this;
        }
        public MockKube end() {
            return MockKube.this;
        }
    }

    public <T extends CustomResource, L extends KubernetesResourceList<T>, D extends Doneable<T>> MockedCrd<T, L, D>
            withCustomResourceDefinition(CustomResourceDefinition crd, Class<T> instanceClass, Class<L> instanceListClass, Class<D> doneableInstanceClass) {
        MockedCrd<T, L, D> mockedCrd = new MockedCrd<>(crd, instanceClass, instanceListClass, doneableInstanceClass);
        this.mockedCrds.add(mockedCrd);
        return mockedCrd;
    }

    interface Assertion {
        void assert_();
    }

    private Map<Class<? extends HasMetadata>, MockBuilder<?, ?, ?, ?>> mockBuilders = new HashMap<>();

    @SuppressWarnings("unchecked")
    public KubernetesClient build() {
        List<Assertion> closeAssertions = new ArrayList<>();
        MockBuilder<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMapMockBuilder = new MockBuilder<>(ConfigMap.class, ConfigMapList.class, DoneableConfigMap.class, MockBuilder.castClass(Resource.class), cmDb);
        closeAssertions.add(configMapMockBuilder::assertNoWatchers);
        MockBuilder<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> endpointMockBuilder = new MockBuilder<>(Endpoints.class, EndpointsList.class, DoneableEndpoints.class, MockBuilder.castClass(Resource.class), endpointDb);
        closeAssertions.add(endpointMockBuilder::assertNoWatchers);
        ServiceMockBuilder serviceMockBuilder = new ServiceMockBuilder(svcDb, endpointDb);
        closeAssertions.add(serviceMockBuilder::assertNoWatchers);
        MockBuilder<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secretMockBuilder = new MockBuilder<>(Secret.class, SecretList.class, DoneableSecret.class, MockBuilder.castClass(Resource.class), secretDb);
        closeAssertions.add(secretMockBuilder::assertNoWatchers);
        MockBuilder<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>> serviceAccountMockBuilder = new MockBuilder<>(ServiceAccount.class, ServiceAccountList.class, DoneableServiceAccount.class, MockBuilder.castClass(Resource.class), serviceAccountDb);
        closeAssertions.add(serviceAccountMockBuilder::assertNoWatchers);
        MockBuilder<Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>> routeMockBuilder = new MockBuilder<>(Route.class, RouteList.class, DoneableRoute.class, MockBuilder.castClass(Resource.class), routeDb);
        closeAssertions.add(routeMockBuilder::assertNoWatchers);
        MockBuilder<PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget, Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>> podDisruptionBudgedMockBuilder = new MockBuilder<>(PodDisruptionBudget.class, PodDisruptionBudgetList.class, DoneablePodDisruptionBudget.class, MockBuilder.castClass(Resource.class), pdbDb);
        closeAssertions.add(podDisruptionBudgedMockBuilder::assertNoWatchers);
        MockBuilder<RoleBinding, RoleBindingList, DoneableRoleBinding, Resource<RoleBinding, DoneableRoleBinding>> roleBindingMockBuilder = new MockBuilder<>(RoleBinding.class, RoleBindingList.class, DoneableRoleBinding.class, MockBuilder.castClass(Resource.class), pdbRb);
        closeAssertions.add(roleBindingMockBuilder::assertNoWatchers);
        MockBuilder<ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding, Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> clusterRoleBindingMockBuilder = new MockBuilder<>(ClusterRoleBinding.class, ClusterRoleBindingList.class, DoneableClusterRoleBinding.class, MockBuilder.castClass(Resource.class), pdbCrb);
        closeAssertions.add(clusterRoleBindingMockBuilder::assertNoWatchers);
        MockBuilder<NetworkPolicy, NetworkPolicyList, DoneableNetworkPolicy, Resource<NetworkPolicy, DoneableNetworkPolicy>> networkPolicyMockBuilder = new MockBuilder<>(NetworkPolicy.class, NetworkPolicyList.class, DoneableNetworkPolicy.class, MockBuilder.castClass(Resource.class), policyDb);
        closeAssertions.add(networkPolicyMockBuilder::assertNoWatchers);

        Map<String, Pod> podDb1 = podDb;
        MockBuilder<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> podMockBuilder = new MockBuilder<>(Pod.class, PodList.class, DoneablePod.class, MockBuilder.castClass(PodResource.class), podDb1);
        closeAssertions.add(podMockBuilder::assertNoWatchers);
        MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods = podMockBuilder.build();

        MockBuilder<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> persistentVolumeClaimMockBuilder = new MockBuilder<>(PersistentVolumeClaim.class, PersistentVolumeClaimList.class, DoneablePersistentVolumeClaim.class, MockBuilder.castClass(Resource.class), pvcDb);
        closeAssertions.add(persistentVolumeClaimMockBuilder::assertNoWatchers);
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPersistentVolumeClaims = persistentVolumeClaimMockBuilder.build();
        DeploymentMockBuilder mockDep = new DeploymentMockBuilder(depDb, mockPods);
        closeAssertions.add(mockDep::assertNoWatchers);
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet,
                RollableScalableResource<StatefulSet, DoneableStatefulSet>> mockSs = buildStatefulSets(podMockBuilder, mockPods, mockPersistentVolumeClaims);

        // Top level group
        KubernetesClient mockClient = mock(KubernetesClient.class);
        configMapMockBuilder.build2(mockClient::configMaps);
        serviceMockBuilder.build2(mockClient::services);
        secretMockBuilder.build2(mockClient::secrets);
        serviceAccountMockBuilder.build2(mockClient::serviceAccounts);
        when(mockClient.pods()).thenReturn(mockPods);
        endpointMockBuilder.build2(mockClient::endpoints);
        when(mockClient.persistentVolumeClaims()).thenReturn(mockPersistentVolumeClaims);

        // API group
        AppsAPIGroupDSL api = mock(AppsAPIGroupDSL.class);
        when(mockClient.apps()).thenReturn(api);
        when(api.statefulSets()).thenReturn(mockSs);
        mockDep.build2(api::deployments);

        // Custom Resources
        if (mockedCrds != null && !mockedCrds.isEmpty()) {
            NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition,
                    Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>>
                    crds = mock(NonNamespaceOperation.class);
            for (MockedCrd<?, ?, ?> mockedCrd : this.mockedCrds) {
                CustomResourceDefinition crd = mockedCrd.crd;
                Resource crdResource = mock(Resource.class);
                when(crdResource.get()).thenReturn(crd);
                when(crds.withName(crd.getMetadata().getName())).thenReturn(crdResource);
                when(mockClient.customResources(any(CustomResourceDefinition.class), (Class) eq(mockedCrd.crClass), (Class) eq(mockedCrd.crListClass),
                        (Class) eq(mockedCrd.crDoneableClass))).thenAnswer(invocation -> {
                            CustomResourceDefinition crdArg = invocation.getArgument(0);
                            if (crd.getSpec().getGroup().equals(crdArg.getSpec().getGroup())
                                    && crd.getSpec().getVersion().equals(crdArg.getSpec().getVersion())) {
                                String key = crdArg.getSpec().getGroup() + "##" + crdArg.getSpec().getVersion();
                                CreateOrReplaceable crdMixedOp = crdMixedOps.get(key);
                                if (crdMixedOp == null) {
                                    CustomResourceMockBuilder customResourceMockBuilder = new CustomResourceMockBuilder<>((MockedCrd) mockedCrd);
                                    closeAssertions.add(customResourceMockBuilder::assertNoWatchers);
                                    crdMixedOp = (MixedOperation<CustomResource, ? extends KubernetesResource, Doneable<CustomResource>, Resource<CustomResource, Doneable<CustomResource>>>) customResourceMockBuilder.build();
                                    crdMixedOps.put(key, crdMixedOp);
                                }
                                return crdMixedOp;
                            } else {
                                throw new RuntimeException("Unknown CRD " + invocation.getArgument(0));
                            }
                        });
            }
            when(mockClient.customResourceDefinitions()).thenReturn(crds);
        }

        // Network group
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        when(mockClient.network()).thenReturn(network);
        networkPolicyMockBuilder.build2(network::networkPolicies);

        // Policy group
        PolicyAPIGroupDSL policy = mock(PolicyAPIGroupDSL.class);
        when(mockClient.policy()).thenReturn(policy);
        podDisruptionBudgedMockBuilder.build2(mockClient.policy()::podDisruptionBudget);

        // RBAC group
        RbacAPIGroupDSL rbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(rbac);
        roleBindingMockBuilder.build2(mockClient.rbac()::roleBindings);
        clusterRoleBindingMockBuilder.build2(mockClient.rbac()::clusterRoleBindings);

        // Openshift group
        OpenShiftClient mockOpenShiftClient = mock(OpenShiftClient.class);
        when(mockClient.adapt(OpenShiftClient.class)).thenReturn(mockOpenShiftClient);
        routeMockBuilder.build2(mockOpenShiftClient::routes);

        mockHttpClient(mockClient);

        doAnswer(i -> {
            for (Assertion a : closeAssertions) {
                a.assert_();
            }
            return null;
        }).when(mockClient).close();
        return mockClient;
    }

    public <T extends HasMetadata> void assertNumWatchers(Class<T> c, int expectedNumWatchers) {
        MockBuilder<?, ?, ?, ?> mockBuilder = mockBuilders.get(c);
        if (mockBuilder == null) {
            throw new AssertionError("Unknown resource " + c);
        }
        mockBuilder.assertNumWatchers(expectedNumWatchers);
    }

    private void mockHttpClient(KubernetesClient mockClient)   {
        // The CRD status update is build on the HTTP client directly since it is not supported in Fabric8.
        // We have to mock the HTTP client to make it pass.
        URL fakeUrl = null;
        try {
            fakeUrl = new URL("http", "my-host", 9443, "/");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        OkHttpClient mockedOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(OkHttpClient.class)).thenReturn(mockedOkHttp);
        Call mockedCall = mock(Call.class);
        when(mockedOkHttp.newCall(any(Request.class))).thenReturn(mockedCall);
        Response response = new Response.Builder().code(200).request(new Request.Builder().url(fakeUrl).build()).message("HTTP OK").protocol(Protocol.HTTP_1_1).build();
        try {
            when(mockedCall.execute()).thenReturn(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>>
        buildStatefulSets(MockBuilder<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> podMockBuilder, MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods,
                          MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim,
                                  Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs) {
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet,
                DoneableStatefulSet>> result = new StatefulSetMockBuilder(podMockBuilder, ssDb, podDb, mockPods, mockPvcs).build();
        return result;
    }


    private static <T extends HasMetadata, D extends Doneable<T>> Map<String, T> db(Collection<T> initialResources, Class<T> cls, Class<D> doneableClass) {
        return new ConcurrentHashMap<>(initialResources.stream().collect(Collectors.toMap(
            c -> c.getMetadata().getName(),
            c -> copyResource(c, cls, doneableClass))));
    }

    @SuppressWarnings("unchecked")
    private static <T extends HasMetadata, D extends Doneable<T>> T copyResource(T resource, Class<T> resourceClass, Class<D> doneableClass) {
        try {
            D doneableInstance = doneableClass.getDeclaredConstructor(resourceClass).newInstance(resource);
            T done = (T) Doneable.class.getMethod("done").invoke(doneableInstance);
            return done;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
