/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;

public class KafkaExporterCertificateOutputWriter extends AbstractCertificateOutputWriter<EntityOperatorIdentity> {

    public KafkaExporterCertificateOutputWriter(Resource<Secret> secretResource) {
        super(EntityOperatorIdentity.class, secretResource);
    }

    @Override
    protected String p12Key(EntityOperatorIdentity entityOperatorIdentity) {
        throw new RuntimeException("TODO");
    }

    @Override
    protected String passwordKey(EntityOperatorIdentity entityOperatorIdentity) {
        return null;
    }

    @Override
    protected String certKey(EntityOperatorIdentity entityOperatorIdentity) {
        return null;
    }

    @Override
    protected String keyKey(EntityOperatorIdentity entityOperatorIdentity) {
        return null;
    }
}
