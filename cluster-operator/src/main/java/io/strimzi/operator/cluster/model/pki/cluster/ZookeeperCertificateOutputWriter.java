/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.pki.AbstractCertificateOutputWriter;
import io.strimzi.operator.cluster.model.pki.NamedResource;
import io.strimzi.operator.common.Reconciliation;

public class ZookeeperCertificateOutputWriter extends AbstractCertificateOutputWriter<ZooKeeperNodeIdentity> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerSubtypes(
                new NamedType(ZooKeeperNodeIdentity.class, "zookeeper"));
    }

    private final String cluster;
    private final NamedResource<Secret> statusResource;
    private final NamedResource<Secret> issuedResource;

    public ZookeeperCertificateOutputWriter(Reconciliation reconciliation,
                                            String cluster,
                                            NamedResource<Secret> statusResource,
                                            NamedResource<Secret> issuedResource,
                                            OwnerReference ownerReference) {
        super(reconciliation,  null, ownerReference, MAPPER);
        this.statusResource = statusResource;
        this.issuedResource = issuedResource;
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

    @Override
    protected String stateKey(ZooKeeperNodeIdentity id) {
        return ZookeeperCluster.zookeeperPodName(cluster, ((ZooKeeperNodeIdentity) id).nodeId()) + ".state";
    }

    @Override
    protected NamedResource<Secret> statusResource(ZooKeeperNodeIdentity zooKeeperNodeIdentity) {
        return statusResource;
    }

    @Override
    protected NamedResource<Secret> issuedResource(ZooKeeperNodeIdentity zooKeeperNodeIdentity) {
        return issuedResource;
    }
}
