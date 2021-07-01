/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.common.Reconciliation;

public class KafkaExporterCertificateOutputWriter extends AbstractCertificateOutputWriter<EntityOperatorIdentity> {

    public KafkaExporterCertificateOutputWriter(Reconciliation reconciliation, NamedResource<Secret> secretResource) {
        super(reconciliation, EntityOperatorIdentity.class, secretResource, "kafka-exporter");
    }
}
