/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;

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
 * </code></pre>
 * for consumption by broker containers.
 *
 * @see LegacyBrokersCertificateOutputWriter for the old-style support
 */
public class NewBrokersCertificateOutputWriter extends AbstractCertificateOutputWriter<BrokerIdentity> {

    NewBrokersCertificateOutputWriter(Resource<Secret> secretResource) {
        super(BrokerIdentity.class, secretResource, null);
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
        return null;
    }

    @Override
    protected String keyKey(BrokerIdentity id) {
        return null;
    }

}
