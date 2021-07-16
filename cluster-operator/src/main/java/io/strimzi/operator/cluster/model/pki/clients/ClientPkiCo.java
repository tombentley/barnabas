/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.LegacyTrustOutputWriter;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustPrivateState;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

public class ClientPkiCo {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ClientPkiCo.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(KafkaUserIdentity.class, "user"));
    }

    private final KubernetesClient client;
    private final Clock clock;
    private final CertManager certManager;

    ClientPkiCo(KubernetesClient client, CertManager certManager, Clock clock) {
        this.clock = clock;
        this.client = client;
        this.certManager = certManager;
    }

    public static class BootstrapResult {
        private final TrustPrivateState trustPrivateState;
        private final Reconciliation reconciliation;
        private final String namespace;
        private final String cluster;
        private final Set<TrustRoot> newTrustRoots;
        private final Set<TrustRoot> usedTrustRoots;
        private final HashSet<TrustRoot> existingTrust;

        public BootstrapResult(TrustPrivateState trustPrivateState,
                               Reconciliation reconciliation,
                               String namespace,
                               String cluster,
                               Set<TrustRoot> newTrustRoots,
                               Set<TrustRoot> usedTrustRoots) {
            this.trustPrivateState = trustPrivateState;
            this.reconciliation = reconciliation;
            this.namespace = namespace;
            this.cluster = cluster;
            this.newTrustRoots = newTrustRoots;
            this.usedTrustRoots = usedTrustRoots;
            existingTrust = new HashSet<>(trustPrivateState.load().values());
        }
    }

    public BootstrapResult bootstrapCa(Reconciliation reconciliation,
                                       String namespace, String cluster,
                                       CertificateAuthority clientsCa,
                                       OwnerReference ownerReference) throws CertificateException {
        Objects.requireNonNull(namespace);
        Objects.requireNonNull(cluster);
        if (clientsCa == null) {
            clientsCa = new CertificateAuthority();
            clientsCa.setValidityDays(365);
            clientsCa.setRenewalDays(30);
        }
        long t0 = System.nanoTime();
        Chain chain = new Chain(reconciliation, clock,
                client.secrets().inNamespace(namespace),
                certManager);

        // TODO fix the name
        TrustPrivateState trustPrivateState = new TrustPrivateState(
                ownerReference,
                new NamedResource<>(client.secrets().inNamespace(namespace), cluster + "-clients-pki-state"));
        chain.reconcile(Chain.clusterCaFromCrd(cluster, clientsCa),
                clientsCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);

        final Set<TrustRoot> newTrustRoots = new HashSet<>();
        final Set<TrustRoot> usedTrustRoots = new HashSet<>();
        return new BootstrapResult(trustPrivateState, reconciliation, namespace, cluster, newTrustRoots, usedTrustRoots);
    }

    // TODO this needs a test
    public void trust(BootstrapResult result)
            throws CertificateEncodingException {

        TrustController trustController = new TrustController(result.reconciliation,
                result.trustPrivateState,
                List.of(new LegacyTrustOutputWriter(client.secrets().inNamespace(result.namespace)
                        .withName(KafkaResources.clusterCaCertificateSecretName(result.cluster)))));

        Comparator<TrustRoot> comparator = Comparator.comparing(x -> x.fingerprint());
        Set<TrustRoot> newState = result.trustPrivateState.load().values().stream().map(trustRoot -> {
            if (result.newTrustRoots.contains(trustRoot)) {
                // TODO this is fishy:
                // a) what is enforcing the valid transitions
                // b) why the need for the resourceVersion argument, then the TrustRoot already knows its rv?
                // c) why is it only toTrusted that needs the rv?
                return trustRoot.toTrusted(trustRoot.resourceVersion());
            } else {
                return trustRoot;
            }
        }).collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
        Set<TrustRoot> toPhaseOut = new TreeSet<>(comparator);
        toPhaseOut.removeAll(result.usedTrustRoots);
        newState.addAll(toPhaseOut.stream().map(TrustRoot::toPhaseOut).collect(Collectors.toSet()));
        // If a new CA cert was used this will
        trustController.reconcile(newState);
    }
}


