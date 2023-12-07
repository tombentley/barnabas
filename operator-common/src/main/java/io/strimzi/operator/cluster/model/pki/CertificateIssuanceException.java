/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public class CertificateIssuanceException extends RuntimeException {
    public CertificateIssuanceException() {
        this(null, null);
    }

    public CertificateIssuanceException(String message) {
        this(message, null);
    }

    public CertificateIssuanceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CertificateIssuanceException(Throwable cause) {
        this(null, cause);
    }
}
