/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;

public class KafkaExporterCertificateOutputWriter extends AbstractCertificateOutputWriter<EntityOperatorIdentity> {

    public KafkaExporterCertificateOutputWriter(Resource<Secret> secretResource) {
        super(EntityOperatorIdentity.class, secretResource, "kafka-exporter");
    }
}
