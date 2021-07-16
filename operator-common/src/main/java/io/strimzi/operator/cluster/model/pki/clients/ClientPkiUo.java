/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.CertificateController;
import io.strimzi.operator.cluster.model.pki.CertificateIssuer;
import io.strimzi.operator.cluster.model.pki.CertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.CertificateReconcileResult;
import io.strimzi.operator.cluster.model.pki.Chain;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.cluster.model.pki.RequiredCertificate;
import io.strimzi.operator.cluster.model.pki.TrustPrivateState;
import io.strimzi.operator.cluster.model.pki.TrustRoot;
import io.strimzi.operator.cluster.model.pki.TrustState;
import io.strimzi.operator.cluster.model.pki.Validator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

/**
 * Unlike the ClusterPki, the UO part of the ClientsPki is just the certificate controller part.
 * The trust in root CA certificates is maintained by the CO.
 * Therefore this just observes issued certificates and awaits certain state changes in the trust private state Secret
 * which are performed to the CO.
 * <p>
 * 1. When the UO issues a certificate it may create a new root cert in the trust secret
 * in the {@link TrustState#UNTRUSTED} state.
 * The CO rolls the brokers and transitions it to the {@link TrustState#TRUSTED} state
 * <p>
 * 2. When a pending end entity certificate has a trusted root cert
 * the UO can then add the issued certificate to the user secret.
 * <p>
 * 3. When the root cert enters the renewal period the CO transitions it to PHASE_OUT.
 * <p>
 * 4. That should cause the UO to issue new end-entity certificates if it hasn't already.
 * <p>
 * 5. When the root certificate has expired the CO removes it from the trust state.
 */
public class ClientPkiUo {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ClientPkiUo.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(KafkaUserIdentity.class, "user"));
    }

    private final KubernetesClient client;
    private final Clock clock;
    private final CertManager certManager;

    ClientPkiUo(KubernetesClient client, CertManager certManager, Clock clock) {
        this.clock = clock;
        this.client = client;
        this.certManager = certManager;
    }

    static class BootstrapResult {
        public final Reconciliation reconciliation;
        public final CertificateIssuer issuer;
        public final CertificateController certificateController;
        private final KubernetesClient client;
        private final String namespace;
        private final String cluster;

        public BootstrapResult(Reconciliation reconciliation,
                               CertificateIssuer issuer,
                               CertificateController certificateController,
                               KubernetesClient client,
                               String namespace,
                               String cluster) {
            this.reconciliation = reconciliation;
            this.issuer = issuer;
            this.certificateController = certificateController;
            this.client = client;
            this.namespace = namespace;
            this.cluster = cluster;
        }

        public void issue(KafkaUser user) throws CertificateException {

            OwnerReference ownerReference = null;
            CertificateOutputWriter<KafkaUserIdentity> certificateOutputWriter = new AbstractCertificateOutputWriter<KafkaUserIdentity>(reconciliation,
                    null,
                    ownerReference,
                    MAPPER) {

                @Override
                protected String stateKey(KafkaUserIdentity subjectIdentity) {
                    return "user.state";
                }

                @Override
                protected NamedResource<Secret> statusResource(KafkaUserIdentity subjectIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), user.getCRDName());
                }

                @Override
                protected NamedResource<Secret> issuedResource(KafkaUserIdentity subjectIdentity) {
                    return new NamedResource<>(client.secrets().inNamespace(namespace), user.getCRDName());
                }
            };

            CertificateReconcileResult reconcile = certificateController.reconcile(new RequiredCertificate<>(new KafkaUserIdentity(user.getCRDName()),
                    new Subject.Builder().withCommonName(user.getCRDName()).build()), certificateOutputWriter);
        }
    }

    public BootstrapResult bootstrap(Reconciliation reconciliation,
                                     String namespace,
                                     String cluster,
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
                new NamedResource<>(client.secrets().inNamespace(namespace), cluster + "-cluster-pki-state"));
        // TODO we don't ourselves reconcile the chain here. We somehow need to instantiate an issuer based on
        // what's present in the Secrets and/or the Kafka CR
        // Does this that Chain ought to be composed of two parts
        // First it creates the certs and persists them in Secrets
        // Second is instantiates a CertificateIssuer from the Secrets
        // For the ClusterPki both of those would happen in the CO
        // For the ClientsPki only the first would happen in the CO
        // the second would happen in the UO, since that's where we need to instantiate the secrets.
        CertificateIssuer issuer = chain.reconcile(Chain.clusterCaFromCrd(cluster, clientsCa),
                clientsCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        LOGGER.debugCr(reconciliation, "Chain reconciliation took {}ms",
                (System.nanoTime() - t0) / 1_000_000.0);
        Validator validator = new Validator(clock, clientsCa.getRenewalDays());
        CertificateController controller = new CertificateController(reconciliation, trustPrivateState,
                issuer,
                validator, clientsCa.getValidityDays(),
                clientsCa.isGenerateSecretOwnerReference() ? ownerReference : null);
        final Set<TrustRoot> newTrustRoots = new HashSet<>();
        final Set<TrustRoot> usedTrustRoots = new HashSet<>();
        return new BootstrapResult(reconciliation, issuer, controller, client, namespace, cluster);
    }


}
