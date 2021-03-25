/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * Writes a secret like:
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: ...
 * data:
 *   # Entries for the current secret
 *   broker-$NUM.state: ...    # The complete state, possibly as JSON?
 * </code></pre>
 */
public class BrokersCertificateStatusStore implements CertificateStatusStore {

    private final Resource<Secret> secretResource;
    private final ObjectMapper mapper;

    public BrokersCertificateStatusStore(ObjectMapper mapper, Resource<Secret> secretResource) {
        this.mapper = mapper;
        this.secretResource = secretResource;
    }

    @Override
    public CertificateStatus loadStatus(SubjectIdentity subjectIdentity) {
        Secret secret = secretResource.get();
        if (secret == null || secret.getData() == null || secret.getData().isEmpty()) {
            return CertificateStatus.createNew(subjectIdentity);
        } else {
            String s = secret.getData().get(stateKey((BrokerIdentity) subjectIdentity));// TODO get rid of this cast
            if (s == null) {
                return CertificateStatus.createNew(subjectIdentity);
            } else {
                try {
                    return mapper.readValue(s, CertificateStatus.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    String stateKey(BrokerIdentity id) {
        return "broker-" + id.brokerId() + ".state";
    }

    @Override
    public void storeStatus(CertificateStatus c) {
        BrokerIdentity id = (BrokerIdentity) c.subjectIdentity();
        Secret secret = secretResource.get();
        SecretBuilder builder = secret != null ? new SecretBuilder(secret) : new SecretBuilder();
        try {
            builder.addToData(stateKey(id), mapper.writeValueAsString(c));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        secretResource.createOrReplace(builder.build());
    }
}
