/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public class CertificateIssuanceException extends RuntimeException {
    public CertificateIssuanceException() {
    }

    public CertificateIssuanceException(String message) {
        super(message);
    }

    public CertificateIssuanceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CertificateIssuanceException(Throwable cause) {
        super(cause);
    }
}
