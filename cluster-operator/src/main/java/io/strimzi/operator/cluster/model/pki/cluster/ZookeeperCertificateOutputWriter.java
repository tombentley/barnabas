/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;

public class ZookeeperCertificateOutputWriter extends AbstractCertificateOutputWriter<ZooKeeperNodeIdentity> {

    public ZookeeperCertificateOutputWriter(Resource<Secret> secretResource) {
        super(ZooKeeperNodeIdentity.class, secretResource, null);
    }

    @Override
    protected String p12Key(ZooKeeperNodeIdentity id) {
        return "my-cluster-zookeeper-" + id.nodeId() + ".p12";
    }

    @Override
    protected String passwordKey(ZooKeeperNodeIdentity id) {
        return "my-cluster-zookeeper-" + id.nodeId() + ".password";
    }

    @Override
    protected String certKey(ZooKeeperNodeIdentity id) {
        return "my-cluster-zookeeper-" + id.nodeId() + ".crt";
    }

    @Override
    protected String keyKey(ZooKeeperNodeIdentity id) {
        return "node-" + id.nodeId() + ".key";
    }
}
