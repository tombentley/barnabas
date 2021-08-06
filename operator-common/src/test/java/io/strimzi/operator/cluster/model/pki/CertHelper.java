/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


public class CertHelper {
    public static X509Certificate getX509Certificate(String s) throws IOException, CertificateException {
        InputStream resourceAsStream = PkiUtilsTest.class.getResourceAsStream(s);
        byte[] buffer = resourceAsStream.readAllBytes();
        return PkiUtils.x509Certificate(buffer);
    }
}