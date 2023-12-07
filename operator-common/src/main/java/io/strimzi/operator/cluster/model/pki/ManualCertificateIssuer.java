/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.api.model.events.v1.EventBuilder;
import io.fabric8.kubernetes.api.model.events.v1.EventList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.certs.Subject;

/**
 * A manual certificate issuer, where the operator relies on a user to populate a Secret with certificates that
 * they have issued or obtained externally from Strimzi.
 *
 * The user would create (and lock down!) the Secret.
 * The user would then configure Strimzi to use the manual issuer, pointing it at this Secret.
 * When a cert was needed the operator would create an Event requesting it issuance.
 * The Secret entries would be
 *   ${subject}.key would provide the key
 *   ${subject}.crt would provide the keychain
 * {@link #pollForRequestedCertificate(byte[])} would check the Secret and if necessary create another Event.
 * Once the Secret was populated {@link #pollForRequestedCertificate(byte[])} would return it and removce those entries from the Secret.
 */
public class ManualCertificateIssuer implements CertificateIssuer {
    private final MixedOperation<Secret, SecretList, Resource<Secret>> secrets;
    private final MixedOperation<Event, EventList, Resource<Event>> events;
    private final String namespace;
    private final String secretName;

    public ManualCertificateIssuer(KubernetesClient client, String namespace, String secretName) {
        this.namespace = namespace;
        this.secretName = secretName;
        this.secrets = client.secrets();
        this.events = client.events().v1().events();
    }

    static class Token {
        private final String eventName;
        private final Subject subject;
        private final int requestedValidity;

        public Token(String eventName, Subject subject, int requestedValidity) {
            this.eventName = eventName;
            this.subject = subject;
            this.requestedValidity = requestedValidity;
        }
    }

    @Override
    public byte[] requestCertificate(Subject subject, int requestedValidity) throws CertificateIssuanceException {
        Secret secret = secrets.inNamespace(namespace).withName(secretName).get();
        if (secret == null) {

        }
        // emit a kube event asking the user to issue this certificate
        Event event = events.inNamespace(namespace).withName("").createOrReplace(
                new EventBuilder()
                        .withNewMetadata()
                            .withGenerateName("")
                        .endMetadata()
                        .withNewRegarding()
                        .withKind("Secret")
                        .withNamespace(namespace)
                        .withName(secretName)
                        .withApiVersion(secret.getApiVersion())
                        .withUid(secret.getMetadata().getUid())
                        .withFieldPath("data")
                        .endRegarding()
                        .withNewRelated()// TODO link to the Kafka resource?
                        .endRelated()
                        .withNote(String.format("A PEM encoded certificate is required in data.%s\\.crt and the corresponding key in data.%s\\.key. " +
                                        "The certificate should have the following SubjectAlternativeNames: %s",
                                subject.toString(), subject.toString(),
                                subject.subjectAltNames()))
                        .withReason("CertificateRequired")
                        .withNewSeries()
                            .withCount(1)
                            .withLastObservedTime(new MicroTime())
                        .endSeries()
                        .build());
        // serialize the subject, validity, event reference
        try {
            return new ObjectMapper().writeValueAsBytes(new Token(event.getMetadata().getName(), subject, requestedValidity));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<IssuedCertificate> pollForRequestedCertificate(byte[] tokenBytes) throws CertificateIssuanceException {
        //  deserialize the token
        Token token = null;
        try {
            token = new ObjectMapper().readValue(tokenBytes, Token.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Resource<Event> eventResource = events.inNamespace(namespace).withName(token.eventName);
        // get the secret
        Secret secret = secrets.inNamespace(namespace).withName(secretName).get();
        if (secret == null) {
            // TODO increment the event count
            return Optional.empty();
        } else {
            String s = secret.getData().get(token.subject + ".crt");
            if (s == null) {
                // TODO increment the event count
                return Optional.empty();
            } else {
                // TODO that the chain is well formed
                // TODO that the end entity certificate is not CA: true
                // TODO that the certificate is PKIX-valid now
                // TODO that the given key is a pair for the certificate
                // TODO delete the existing event
                eventResource.delete();
                // TODO if it's not valid create a new event and return empty
                // TODO otherwise it is valid, to return the issued cert
                return Optional.of(null);
            }
        }
    }
}
