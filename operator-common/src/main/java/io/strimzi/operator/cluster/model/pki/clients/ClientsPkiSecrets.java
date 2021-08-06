/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki.clients;

import io.strimzi.api.kafka.model.KafkaResources;

public final class ClientsPkiSecrets {
    private ClientsPkiSecrets() {
    }

    public static String internalIssuerCerts(String cluster) {
        // secret name: ${Kafka.metadata.name}-clients-ca-cert
        //   ca.crt
        //   ca.p12
        //   ca.password
        return KafkaResources.clientsCaCertificateSecretName(cluster);
    }

    public static String internalIssuerKeys(String cluster) {
        // secret name: ${Kafka.metadata.name}-clients-ca
        //   ca.key
        return KafkaResources.clientsCaKeySecretName(cluster);
    }

    public static String user(String userName) {
        // secret name: ${KafkaUser.metadata.name}
        //   user.key -- the user's key in PEM format
        //   user.crt -- the user's cert in PEM format
        //   user.p12 -- the user's cert and key in pkcxs#12 format
        //   user.password -- for the p12
        //   ca.crt -- the cluster CA cert in PEM format to be trusted
        return userName;
    }
}
