/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

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

    private final String cluster;
    private final boolean includePem;
    private final boolean includeP12;

    BrokersCertificateOutputWriter(Reconciliation reconciliation,
                                   String cluster,
                                   NamedResource<Secret> secretResource,
                                   boolean includePem,
                                   boolean includeP12) {
        super(reconciliation, BrokerIdentity.class, secretResource, null);
        this.cluster = cluster;
        this.includePem = includePem;
        this.includeP12 = includeP12;
    }

    @Override
    protected String p12Key(BrokerIdentity id) {
        return includeP12 ? KafkaCluster.kafkaPodName(cluster, id.brokerId()) + ".p12" : null;
    }

    @Override
    protected String passwordKey(BrokerIdentity id) {
        return includeP12 ? KafkaCluster.kafkaPodName(cluster, id.brokerId()) + ".password" : null;
    }

    @Override
    protected String certKey(BrokerIdentity id) {
        return includePem ? KafkaCluster.kafkaPodName(cluster, id.brokerId()) + ".crt" : null;
    }

    @Override
    protected String keyKey(BrokerIdentity id) {
        return includePem ? KafkaCluster.kafkaPodName(cluster, id.brokerId()) + ".key" : null;
    }

}
