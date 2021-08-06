/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Map;

/**
 * Abstraction for things which know how to take a bunch of trust roots
 * and put them somewhere for consumption by containers needing to know what to trust.
 * This includes, but is not limited to, putting them in a Secret.
 */
public interface TrustedCertificateWriter {
    void write(Map<Fingerprint, X509Certificate> trustRoots) throws CertificateEncodingException;
}
