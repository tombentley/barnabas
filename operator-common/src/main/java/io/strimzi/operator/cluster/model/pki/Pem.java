/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static io.strimzi.operator.cluster.model.pki.TrustManager.x509Certificate;

public class Pem {

    /**
     * Parses a PEM encoded certificate chain into a list of certificated.
     * Does not validate that the certificates for a chain.
     * @param pem The PEM encoded certificates.
     * @return The list of certificates.
     * @throws CertificateException
     */
    public static List<X509Certificate> parseCertChain(String pem) throws CertificateException {
        String[] parts = pem.split("-----END CERTIFICATE-----\r?\n" +
                "-----BEGIN CERTIFICATE-----");
        List<X509Certificate> result = new ArrayList<>(parts.length);
        for (String part : parts) {
            result.add(x509Certificate(part.replace("-----END CERTIFICATE-----", "")
                                    .replace("-----BEGIN CERTIFICATE-----", "").getBytes(StandardCharsets.ISO_8859_1)));
        }
        return result;
    }

    /**
     * Convert an X509 certificate to its PEM encoded form.
     * @param x509 The certificate.
     * @return The PEM representation of the certificate.
     * @throws CertificateEncodingException
     */
    public static String asPem(X509Certificate x509) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getEncoder();
        StringBuffer sb = new StringBuffer();
        sb.append("-----BEGIN CERTIFICATE-----\n");
        sb.append(encoder.encodeToString(x509.getEncoded()));
        sb.append("\n-----END CERTIFICATE-----");
        return sb.toString();
    }

    /**
     * Converts a list of X509 certificates (assumed to be a cert chain) into its PEM encoded form.
     * @param x509s The certificates.
     * @return The PEM representation of the certificates.
     * @throws CertificateEncodingException
     */
    public static String asPemCertChain(List<X509Certificate> x509s) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getEncoder();
        StringBuffer sb = new StringBuffer();
        boolean first = false;
        for (X509Certificate x509 : x509s) {
            if (!first) {
                sb.append("\n");
            }
            sb.append("-----BEGIN CERTIFICATE-----\n");
            sb.append(encoder.encodeToString(x509.getEncoded()));
            sb.append("\n-----END CERTIFICATE-----");
        }
        return sb.toString();
    }

}
