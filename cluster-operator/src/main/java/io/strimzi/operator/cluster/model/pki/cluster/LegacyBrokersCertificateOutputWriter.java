/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
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
 *   $CLUSTER-kafka-$NUM.p12: ...      # PKCS #12 archive file for storing certificates and keys
 *   $CLUSTER-kafka-$NUM.password: ... # Password for protecting the PKCS #12 archive file.
 *   $CLUSTER-kafka-$NUM.crt: ...      # Certificate for Kafka broker pod &lt;num&gt;.
 *   $CLUSTER-kafka-$NUM.key: ...      # Private key for Kafka broker pod &lt;num&gt;.
 * </code></pre>
 * for consumption by broker containers.
 *
 * @see NewBrokersCertificateOutputWriter for the new-style support
 */
public class LegacyBrokersCertificateOutputWriter extends AbstractCertificateOutputWriter<BrokerIdentity> {

    public LegacyBrokersCertificateOutputWriter(Reconciliation reconciliation, NamedResource<Secret> secretResource) {
        super(reconciliation, BrokerIdentity.class, secretResource, null);
    }

    @Override
    protected String p12Key(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".p12";
    }

    @Override
    protected String passwordKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".password";
    }

    @Override
    protected String certKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".crt";
    }

    @Override
    protected String keyKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".key";
    }
}
