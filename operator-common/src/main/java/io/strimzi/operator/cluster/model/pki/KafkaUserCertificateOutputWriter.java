/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;


public class KafkaUserCertificateOutputWriter extends AbstractCertificateOutputWriter<KafkaUserIdentity> {

    public KafkaUserCertificateOutputWriter(Class<KafkaUserIdentity> cls, Resource<Secret> secretResource) {
        super(cls, secretResource);
    }

    @Override
    protected String p12Key(KafkaUserIdentity kafkaUserIdentity) {
        throw new RuntimeException("TODO");
    }

    @Override
    protected String passwordKey(KafkaUserIdentity kafkaUserIdentity) {
        return null;
    }

    @Override
    protected String certKey(KafkaUserIdentity kafkaUserIdentity) {
        return null;
    }

    @Override
    protected String keyKey(KafkaUserIdentity kafkaUserIdentity) {
        return null;
    }
}
