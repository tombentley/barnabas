/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Abstraction for things which know how to take a bunch of {@code IssuedCertificate IssuedCertificates}
 * and put them somewhere for use by components/clients.
 * This includes, but is not limited to, putting them in a Secret.
 */
public interface CertificateOutputWriter {
    // TODO IssuedCertificate maybe isn't quite right.
    // Since the different writers might be only interested in a subset?
    // Or do we just not separate out the brokers from zk etc?
    // But then how does it work for the KafkaUser's case?
    void write(IssuedCertificate certificate) throws CertificateEncodingException;
}

/**
 * Writes a secret like:
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: ...
 * data:
 *   # Entries for the current secret
 *   broker-$NUM.p12: ...      # PKCS #12 archive file for storing current certificates and keys
 *   broker-$NUM.password: ... # Password for protecting the PKCS #12 archive file.
 *   broker-$NUM.state: ...    # The complete state, possibly as JSON?
 * </code></pre>
 */
class NewBrokersCertificateOutputWriter implements CertificateOutputWriter, CertificateStatusStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Resource<Secret> secretResource;

    NewBrokersCertificateOutputWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(IssuedCertificate certificate) throws CertificateEncodingException {
        // TODO
//        BrokerIdentity id = (BrokerIdentity) subjectIdentity;
//        Secret secret = secretResource.get();
//        SecretBuilder builder = new SecretBuilder(secret);
//        try {
//            builder.addToData(p12Key(id), PkiUtils.asP12());
//            builder.addToData(passwordKey(id), password);
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//        secretResource.patch(builder.build());
        throw new RuntimeException("TODO");
    }

    @Override
    public CertificateStatus load(SubjectIdentity subjectIdentity) {
        BrokerIdentity id = (BrokerIdentity) subjectIdentity;

        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return CertificateStatus.createNew(id);
        } else {
            String s = secret.getData().get(stateKey(id));
            if (s == null) {
                return CertificateStatus.createNew(id);
            } else {
                try {
                    return MAPPER.readValue(s, CertificateStatus.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    String stateKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".state";
    }

    String p12Key(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".p12";
    }

    String passwordKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".password";
    }

    @Override
    public void store(SubjectIdentity subjectIdentity, CertificateStatus c) {
        BrokerIdentity id = (BrokerIdentity) subjectIdentity;
        Secret secret = secretResource.get();
        SecretBuilder builder = new SecretBuilder(secret);
        try {
            builder.addToData(stateKey(id), MAPPER.writeValueAsString(c));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        secretResource.patch(builder.build());
    }
}

/**
 * Writes a secret like:
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: ...
 * data:
 *   # Entries for the current secret
 *   $CLUSTER-kafka-$NUM.p12: ...      # PKCS #12 archive file for storing certificates and keys
 *   $CLUSTER-kafka-$NUM.password: ... # Password for protecting the PKCS #12 archive file.
 *   $CLUSTER-kafka-$NUM.crt: ...      # Certificate for Kafka broker pod <num>.
 *   $CLUSTER-kafka-$NUM.key: ...      # Private key for Kafka broker pod <num>.
 * </code></pre>
 */
class LegacyBrokersCertificateOutputWriter implements CertificateOutputWriter {

    private final String cluster;
    private final Resource<Secret> secretResource;


    public LegacyBrokersCertificateOutputWriter(String cluster, Resource<Secret> secretResource) {
        this.cluster = cluster;
        this.secretResource = secretResource;
    }

    @Override
    public void write(IssuedCertificate certificate) throws CertificateEncodingException {

    }
}
//class ZookeeperNodeCertificateOutputWriter implements CertificateOutputWriter {
//
//}
//class EoCertificateOutputWriter {
//
//}
//class CoCertificateOutputWriter {
//
//}
//class KafkaExporterCertificateOutputWriter {
//
//}
//class KafkaUserCertificateOutputWriter implements CertificateOutputWriter {
//
//}