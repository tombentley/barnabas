/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;


public class KafkaUserCertificateOutputWriter extends AbstractCertificateOutputWriter<KafkaUserIdentity> {

    public KafkaUserCertificateOutputWriter(Class<KafkaUserIdentity> cls, Resource<Secret> secretResource) {
        super(cls, secretResource, "user");
    }

    // TODO a user secret also contains the trusted CA certificate(s)

    @Override
    protected String p12Key(KafkaUserIdentity kafkaUserIdentity) {
        return "user.p12";
    }

    @Override
    protected String passwordKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.password";
    }

    @Override
    protected String certKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.crt";
    }

    @Override
    protected String keyKey(KafkaUserIdentity kafkaUserIdentity) {
        return "user.key";
    }
}
