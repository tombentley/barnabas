/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;

public class ZookeeperCertificateOutputWriter extends AbstractCertificateOutputWriter<ZooKeeperNodeIdentity> {

    public ZookeeperCertificateOutputWriter(Resource<Secret> secretResource) {
        super(ZooKeeperNodeIdentity.class, secretResource);
    }

    @Override
    protected String p12Key(ZooKeeperNodeIdentity id) {
        return "node-" + id.nodeId() + ".p12";
    }

    @Override
    protected String passwordKey(ZooKeeperNodeIdentity id) {
        return "node-" + id.nodeId() + ".password";
    }

    @Override
    protected String certKey(ZooKeeperNodeIdentity id) {
        return "node-" + id.nodeId() + ".crt";
    }

    @Override
    protected String keyKey(ZooKeeperNodeIdentity id) {
        return "node-" + id.nodeId() + ".key";
    }
}
