/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public class PkiException extends RuntimeException {

    public PkiException(String message) {
        super(message);
    }

    public PkiException(String message, Throwable cause) {
        super(message, cause);
    }

    public PkiException(Throwable cause) {
        super(cause);
    }
}
