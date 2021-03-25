/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;

public class ClusterOperatorCertificateOutputWriter extends AbstractCertificateOutputWriter<ClusterOperatorIdentity> {

    public ClusterOperatorCertificateOutputWriter(Resource<Secret> secretResource) {
        super(ClusterOperatorIdentity.class, secretResource);
    }

    @Override
    protected String p12Key(ClusterOperatorIdentity clusterOperatorIdentity) {
        throw new RuntimeException("TODO");
    }

    @Override
    protected String passwordKey(ClusterOperatorIdentity clusterOperatorIdentity) {
        return null;
    }

    @Override
    protected String certKey(ClusterOperatorIdentity clusterOperatorIdentity) {
        return null;
    }

    @Override
    protected String keyKey(ClusterOperatorIdentity clusterOperatorIdentity) {
        return null;
    }
}
