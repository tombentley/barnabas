/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

public class RestoreException extends RuntimeException {
    public RestoreException() {
    }

    public RestoreException(String message) {
        super(message);
    }

    public RestoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestoreException(Throwable cause) {
        super(cause);
    }
}
