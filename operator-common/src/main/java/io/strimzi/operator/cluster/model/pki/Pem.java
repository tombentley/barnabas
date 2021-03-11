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

/**
 * Helper functions for PEM encoding
 */
final class Pem {

    private Pem() {
    }

    /**
     * Parses a PEM encoded certificate chain into a list of certificated.
     * @param pem The PEM encoded certificates.
     * @return The list of certificates.
     * @throws CertificateException
     */
    public static List<X509Certificate> parseCertChain(String pem) throws CertificateException {
        String[] parts = pem.split("-----END CERTIFICATE-----\r?\n" +
                "-----BEGIN CERTIFICATE-----");
        List<X509Certificate> result = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String replace = part.replace("\n-----END CERTIFICATE-----", "")
                    .replace("-----BEGIN CERTIFICATE-----\n", "");
            result.add(x509Certificate(("-----BEGIN CERTIFICATE-----\n" + replace + "\n-----END CERTIFICATE-----").getBytes(StandardCharsets.ISO_8859_1)));
        }
        return checkChain(result);
    }

    /**
     * Checks that the given for a certificate chain, where each certificate is followed by the certificate if it's issuer.
     * @param certs The certs which should form a chain.
     * @return The same list of certificates.
     * @throws RuntimeException if the certificates do not form a chain.
     */
    public static List<X509Certificate> checkChain(List<X509Certificate> certs) {
        X509Certificate last = null;
        for (int i = 0; i < certs.size(); i++) {
            X509Certificate cert = certs.get(i);
            if (last != null) {
                if (!cert.getSubjectDN().equals(last.getIssuerDN())) {
                    throw new RuntimeException("Certificates do not form a chain: " +
                            "The certificate at index " + (i - 1) + " was issued by " + last.getIssuerDN() +
                            "But the certificate at index " + i + " has subject " + cert.getSubjectDN());
                }
            }
            last = cert;
        }
        return certs;
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
     * Converts a list of X509 certificates (a cert chain) into its PEM encoded form.
     * @param x509s The certificates.
     * @return The PEM representation of the certificates.
     * @throws CertificateEncodingException
     */
    public static String asPemCertChain(List<X509Certificate> x509s) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getEncoder();
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (X509Certificate x509 : checkChain(x509s)) {
            if (!first) {
                sb.append("\n");
            }
            first = false;
            sb.append("-----BEGIN CERTIFICATE-----\n");
            sb.append(encoder.encodeToString(x509.getEncoded()));
            sb.append("\n-----END CERTIFICATE-----");
        }
        return sb.toString();
    }

}
