/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.TrustController;
import io.strimzi.operator.cluster.model.pki.TrustManager;
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
    private final Reconciliation reconciliation;
    private String namespace;
    private String cluster;

    ClientPkiCo(KubernetesClient client, CertManager certManager, Clock clock, Reconciliation reconciliation,
                String namespace, String cluster) {
        this.clock = clock;
        this.client = client;
        this.certManager = certManager;
        this.reconciliation = reconciliation;
        this.namespace = namespace;
        this.cluster = cluster;
    }

    public static class BootstrapResult {
        private final Reconciliation reconciliation;
        private final String namespace;
        private final String cluster;

        public BootstrapResult(Reconciliation reconciliation,
                               String namespace,
                               String cluster) {
            this.reconciliation = reconciliation;
            this.namespace = namespace;
            this.cluster = cluster;
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
                certManager, Objects.requireNonNull(null));

        // TODO fix the name
        chain.reconcile(Chain.clusterCaFromCrd(cluster, clientsCa),
                clientsCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);

        return new BootstrapResult(reconciliation, namespace, cluster);
    }

    // TODO this needs a test
    public void trust(BootstrapResult result)
            throws CertificateEncodingException {
        TrustManager trustManager = new TrustController(
                reconciliation,
                new NamedResource<>(client.secrets().inNamespace(namespace), cluster + "-cluster-pki-state"),
                null);
        trustManager.trustRootCerts(trustManager.requiringTrust());
        trustManager.delete(trustManager.zombies());

//        TrustController trustController = new TrustController(result.reconciliation,
//                result.trustPrivateState,
//                List.of(new LegacyTrustOutputWriter(client.secrets().inNamespace(result.namespace)
//                        .withName(KafkaResources.clusterCaCertificateSecretName(result.cluster)))));
//
//        Comparator<TrustRoot> comparator = Comparator.comparing(x -> x.fingerprint());
//        Set<TrustRoot> newState = result.trustPrivateState.load().values().stream().map(trustRoot -> {
//            if (result.newTrustRoots.contains(trustRoot)) {
//                // TODO this is fishy:
//                // a) what is enforcing the valid transitions
//                // b) why the need for the resourceVersion argument, then the TrustRoot already knows its rv?
//                // c) why is it only toTrusted that needs the rv?
//                return trustRoot.toTrusted(trustRoot.resourceVersion());
//            } else {
//                return trustRoot;
//            }
//        }).collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
//        Set<TrustRoot> toPhaseOut = new TreeSet<>(comparator);
//        toPhaseOut.removeAll(result.usedTrustRoots);
//        newState.addAll(toPhaseOut.stream().map(TrustRoot::toPhaseOut).collect(Collectors.toSet()));
//        // If a new CA cert was used this will
//        trustController.reconcile(newState);
    }
}


