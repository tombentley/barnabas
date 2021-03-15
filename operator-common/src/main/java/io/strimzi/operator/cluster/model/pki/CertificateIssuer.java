/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import io.strimzi.certs.Subject;

public interface CertificateIssuer {

    IssuedCertificate issue(Subject subject, int requestedValidity) throws CertificateIssuanceException;
}
