/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.common.Reconciliation;

/**
 * Writes a secret like:
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: ...
 * data:
 *   # Entries for the current secret
 *   broker-$NUM.p12: ...      # PKCS #12 archive file for storing current certificates and keys
 *   broker-$NUM.password: ... # Password for protecting the PKCS #12 archive file.
 *   broker-$NUM.crt: ...      # PEM encoded DER certificate
 *   broker-$NUM.key: ...      # PEM encoded DER key
 * </code></pre>
 * for consumption by broker containers.
 *
 */
public class BrokersCertificateOutputWriter extends AbstractCertificateOutputWriter<BrokerIdentity> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(BrokerIdentity.class, "kafka"));
    }

    private final String cluster;
    private final boolean includePem;
    private final boolean includeP12;
    private final NamedResource<Secret> statusResource;
    private final NamedResource<Secret> issuedResource;

    BrokersCertificateOutputWriter(Reconciliation reconciliation,
                                   String cluster,
                                   NamedResource<Secret> statusResource,
                                   NamedResource<Secret> secretResource,
                                   OwnerReference ownerReference,
                                   boolean includePem,
                                   boolean includeP12) {
        super(reconciliation, ownerReference, MAPPER);
        this.statusResource = statusResource;
        this.issuedResource = secretResource;
        this.cluster = cluster;
        this.includePem = includePem;
        this.includeP12 = includeP12;
    }

    @Override
    protected String p12Key(BrokerIdentity id) {
        return includeP12 ? super.p12Key(id) : null;
    }

    @Override
    protected String passwordKey(BrokerIdentity id) {
        return includeP12 ? super.passwordKey(id) : null;
    }

    @Override
    protected String certKey(BrokerIdentity id) {
        return includePem ? super.certKey(id) : null;
    }

    @Override
    protected String keyKey(BrokerIdentity id) {
        return includePem ? super.keyKey(id) : null;
    }

    @Override
    protected String keyPrefix(BrokerIdentity id) {
        return KafkaCluster.kafkaPodName(cluster, ((BrokerIdentity) id).brokerId());
    }

    @Override
    protected NamedResource<Secret> statusResource(BrokerIdentity brokerIdentity) {
        return statusResource;
    }

    @Override
    protected NamedResource<Secret> issuedResource(BrokerIdentity brokerIdentity) {
        return issuedResource;
    }

}
