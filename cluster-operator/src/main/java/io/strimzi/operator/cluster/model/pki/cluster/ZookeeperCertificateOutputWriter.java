/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.common.Reconciliation;

public class ZookeeperCertificateOutputWriter extends AbstractCertificateOutputWriter<ZooKeeperNodeIdentity> {

    private final String cluster;

    public ZookeeperCertificateOutputWriter(Reconciliation reconciliation, String cluster, NamedResource<Secret> secretResource) {
        super(reconciliation, ZooKeeperNodeIdentity.class, secretResource, null);
        this.cluster = cluster;
    }

    @Override
    protected String p12Key(ZooKeeperNodeIdentity id) {
        return ZookeeperCluster.zookeeperPodName(cluster, id.nodeId()) + ".p12";
    }

    @Override
    protected String passwordKey(ZooKeeperNodeIdentity id) {
        return ZookeeperCluster.zookeeperPodName(cluster, id.nodeId()) + ".password";
    }

    @Override
    protected String certKey(ZooKeeperNodeIdentity id) {
        return ZookeeperCluster.zookeeperPodName(cluster, id.nodeId()) + ".crt";
    }

    @Override
    protected String keyKey(ZooKeeperNodeIdentity id) {
        return ZookeeperCluster.zookeeperPodName(cluster, id.nodeId()) + ".key";
    }
}
