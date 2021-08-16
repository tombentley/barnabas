/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;


public class CertHelper {

    public static byte[] getKey(String s) {
        try {
            InputStream resourceAsStream = PkiUtilsTest.class.getResourceAsStream(s);
            byte[] buffer = resourceAsStream.readAllBytes();
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static X509Certificate getX509Certificate(String s) {
        try {
            InputStream resourceAsStream = PkiUtilsTest.class.getResourceAsStream(s);
            byte[] buffer = resourceAsStream.readAllBytes();
            return PkiUtils.x509Certificate(buffer);
        } catch (IOException | CertificateException e) {
            throw new RuntimeException(e);
        }
    }
    public static IssuedCertificate issuedCertificate() {
        X509Certificate x509Certificate = CertHelper.getX509Certificate("/CLUSTER_CERT.pem");
        return new IssuedCertificate(CertHelper.getKey("/CLUSTER_KEY.pem"), x509Certificate, List.of(x509Certificate), x509Certificate);
    }
}