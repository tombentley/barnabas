/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public interface CertificateStatusStore {
    CertificateStatus loadStatus(SubjectIdentity subjectIdentity);
    void storeStatus(CertificateStatus c);
}
