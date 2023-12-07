/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public final class ClusterPkiSecrets {
    private ClusterPkiSecrets() {}
    public static String internalIssuerCerts(String cluster) {
        return null;
    }
    public static String internalIssuerKeys(String cluster) {
        return cluster + "-cluster-trust";
    }
    public static String trustState(String cluster) {
        return cluster + "-cluster-trust";
    }
    public static String privateKey(String cluster) {
        return cluster + "-cluster-ca";
    }
    public static String trustRoots(String cluster) {
        return cluster + "-cluster-ca-cert";
    }
    public static String brokers(String cluster) {
        return cluster + "-kafka-brokers";
    }
    public static String zkNodes(String cluster) {
        return cluster + "-zookeeper-nodes";
    }
    public static String entityOperator(String cluster) {
        // entity-operator.key -- the key in PEM format
        // entity-operator.crt -- the cert in PEM format
        // entity-operator.p12 -- the cert and key in pkcxs#12 format
        // entity-operator.password -- for the p12
        // entity-operator.state -- JSON for the CC state machine of this cert. TODO this shouldn't be included when IN_USE
        return cluster + "-entity-operator-certs";
    }
    public static String clusterOperator(String cluster) {
        // cluster-operator.key -- the key in PEM format
        // cluster-operator.crt -- the cert in PEM format
        // cluster-operator.p12 -- the cert and key in pkcxs#12 format
        // cluster-operator.password -- for the p12
        // cluster-operator.state -- JSON for the CC state machine of this cert. TODO this shouldn't be included when IN_USE
        return cluster + "-cluster-operator-certs";
    }
    public static String cruiseControl(String cluster) {
        // cruise-control.key -- the key in PEM format
        // cruise-control.crt -- the cert in PEM format
        // cruise-control.p12 -- the cert and key in pkcxs#12 format
        // cruise-control.password -- for the p12
        // cruise-control.state -- JSON for the CC state machine of this cert. TODO this shouldn't be included when IN_USE
        return cluster + "-cruise-control";
    }
    public static String kafkaExporter(String cluster) {
        // kafka-exporter.key -- the key in PEM format
        // kafka-exporter.crt -- the cert in PEM format
        // kafka-exporter.p12 -- the cert and key in pkcxs#12 format
        // kafka-exporter.password -- for the p12
        // kafka-exporter.state -- JSON for the CC state machine of this cert. TODO this shouldn't be included when IN_USE
        return cluster + "-kafka-exporter";
    }
}
