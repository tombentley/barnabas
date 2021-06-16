/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.util.Collection;

/**
 * Abstraction for things which know how to take a bunch of {@code TrustRoot TrustRoots}
 * and put them somewhere. This includes, but is not limited to, putting them in a Secret for consumption by
 * components needing to know what to trust.
 */
public interface TrustOutputWriter {
    void write(Collection<TrustRoot> trustRoot) throws CertificateEncodingException;
}
