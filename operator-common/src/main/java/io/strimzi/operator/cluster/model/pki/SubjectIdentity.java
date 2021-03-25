/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Identity for a certificate. This is a different thing to the certificate's subject, or {@link io.strimzi.certs.Subject}.
 * It identifies an entity which needs a certificate, independently of the subject DN or SAN for that certificate.
 * This is needed because Strimzi used the same subject DA for all broker certificates, so the subject DN of a
 * certificate tells us nothing.
 *
 */
// This needs to be serializable because it gets persisted in the pending state.
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
//@JsonSubTypes({@JsonSubTypes.Type(value = BrokerIdentity.class, name = "broker")})
public abstract class SubjectIdentity {
    @Override
    public abstract boolean equals(Object other);
    @Override
    public abstract int hashCode();
    @Override
    public abstract String toString();
}

