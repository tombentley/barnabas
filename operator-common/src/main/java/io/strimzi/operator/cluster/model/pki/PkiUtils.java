/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper functions for PEM encoding
 */
final class PkiUtils {

    public static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    public static final String END_CERTIFICATE = "-----END CERTIFICATE-----";

    public static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVAT KEY-----";
    public static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

    private PkiUtils() {
    }

    /**
     * Parses a stream of multiple certificate chain into a list of certificates.
     * @param bytes The encoded certificates.
     * @return The list of certificates.
     * @throws CertificateException If the given {@code bytes} could not be read a sequence of X.509 certificates.
     */
    public static List<X509Certificate> parsePemCertChain(byte[] bytes) throws CertificateException {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        CertificateFactory certificateFactory = x509Factory();
        List<X509Certificate> result = new ArrayList<>();
        while (stream.available() > 0) {
            result.add((X509Certificate) certificateFactory.generateCertificate(stream));
        }
        return checkCertChain(result);
    }

    private static CertificateFactory x509Factory() {
        // CertificateFactory is not documented as thread-safe, so we can't use a constant
        CertificateFactory certificateFactory;
        try {
            certificateFactory = CertificateFactory.getInstance("X509");
        } catch (CertificateException e) {
            throw new RuntimeException(e); // X509 should always be supported
        }
        return certificateFactory;
    }

    /**
     * Checks that the given for a certificate chain, where each certificate is followed by the certificate if it's issuer.
     * @param certs The certs which should form a chain.
     * @return The same list of certificates.
     * @throws RuntimeException if the certificates do not form a chain.
     */
    public static List<X509Certificate> checkCertChain(List<X509Certificate> certs) {
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
     * @throws CertificateEncodingException If the certificate could not be encoded.
     */
    public static String asPemString(X509Certificate x509) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getEncoder();
        return BEGIN_CERTIFICATE + '\n' +
                encoder.encodeToString(x509.getEncoded()) +
                '\n' + END_CERTIFICATE;
    }

    public static byte[] asPemBytes(X509Certificate x509) throws CertificateEncodingException {
        ByteBuffer byteBuffer = asPemBuffer(x509);
        byte[] result = new byte[byteBuffer.remaining()];
        byteBuffer.get(result);
        return result;
    }

    public static String asPemKey(byte[] key) {
        Base64.Encoder encoder = Base64.getEncoder();
        return BEGIN_PRIVATE_KEY + '\n' +
                encoder.encodeToString(key) +
                '\n' + END_PRIVATE_KEY;
    }

    public static ByteBuffer asPemBuffer(X509Certificate x509) throws CertificateEncodingException {
        byte[] asn1 = x509.getEncoded();
        int worseCaseSize = (int)(asn1.length / 3.0 + 1) * 4
                + BEGIN_CERTIFICATE.length() // each char therein will encode to 1 byte
                + END_CERTIFICATE.length() // each char therein will encode to 1 byte
                + 2; // for the two \n
        var buf = ByteBuffer.allocate(worseCaseSize)
                .put(StandardCharsets.ISO_8859_1.encode(BEGIN_CERTIFICATE))
                .put((byte) '\n')
                .put(Base64.getEncoder().encode(asn1))
                .put((byte) '\n')
                .put(StandardCharsets.ISO_8859_1.encode(END_CERTIFICATE));
        buf.flip();
        return buf;
    }

    /**
     * Write the given certificate#s PEM encoded representation to the given {@code path}.
     * @param path The path to write to.
     * @param cert The certificate to write.
     * @return the path.
     * @throws IOException The file could not be written.
     * @throws CertificateEncodingException The cert could not be encoded.
     */
    public static Path writePem(Path path, X509Certificate cert) throws IOException, CertificateEncodingException {
        SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE);
        ByteBuffer src = PkiUtils.asPemBuffer(cert);
        while (src.remaining() > 0) {
            seekableByteChannel.write(src);
        }
        return path;
    }

    /**
     * Converts a list of X509 certificates (a cert chain) into its PEM encoded form.
     * @param x509s The certificates.
     * @return The PEM representation of the certificates.
     * @throws CertificateEncodingException If any of the certificates could not be encoded.
     */
    public static String asPemCertChain(List<X509Certificate> x509s) throws CertificateEncodingException {
        Base64.Encoder encoder = Base64.getEncoder();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (X509Certificate x509 : checkCertChain(x509s)) {
            if (!first) {
                sb.append("\n");
            }
            first = false;
            sb.append(BEGIN_CERTIFICATE).append("\n");
            sb.append(encoder.encodeToString(x509.getEncoded()));
            sb.append("\n").append(END_CERTIFICATE);
        }
        return sb.toString();
    }

    public static Fingerprint fingerprint(X509Certificate x509Certificate) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA1");
            return new Fingerprint(sha1.digest(x509Certificate.getEncoded()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // SHA1 should always be supported
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e); // Should never happen
        }
    }

    /**
     * Parse a certificate from bytes.
     * @param cert The certificate in binary DER or base 64 encoded with PEM header and footer.
     * @return The certificate.
     * @throws CertificateException If the given bytes could not be read as an X.509 certificate.
     */
    public static X509Certificate x509Certificate(byte[] cert) throws CertificateException {
        return x509Certificate(new ByteArrayInputStream(cert)); // no need to close a BAIS
    }

    /**
     * Parse a certificate from a stream.
     * @param stream The certificate in binary DER or Base64 encoded with PEM header and footer.
     * @return The certificate.
     * @throws CertificateException If the given bytes could not be read as an X.509 certificate.
     */
    private static X509Certificate x509Certificate(InputStream stream) throws CertificateException {
        // CertificateFactory is not documented as thread-safe, so we can't use a constant
        CertificateFactory certificateFactory = x509Factory();
        return (X509Certificate) certificateFactory.generateCertificate(stream);
    }

    /**
     * Read a single X509 certificate from the file at the given path.
     * @param path The path to read the certificate from. It should be
     *             in binary DER or Base64 encoded with PEM header and footer.
     * @return The certificate
     * @throws IOException Problems reading the file
     * @throws CertificateException The data in the file could not be parsed as an X509 certificate.
     */
    public static X509Certificate x509Certificate(Path path) throws CertificateException, IOException {
        try (InputStream stream = Files.newInputStream(path)) {
            return x509Certificate(stream);
        }
    }

    public static Set<String> dnsNames(X509Certificate x509Certificate) throws CertificateParsingException {
        Integer dnsType = Integer.valueOf(2);
        return sansOfType(x509Certificate, dnsType);
    }

    public static Set<String> ipAddresses(X509Certificate x509Certificate) throws CertificateParsingException {
        Integer dnsType = Integer.valueOf(7);
        return sansOfType(x509Certificate, dnsType);
    }

    private static Set<String> sansOfType(X509Certificate x509Certificate, Integer dnsType) throws CertificateParsingException {
        Collection<List<?>> subjectAlternativeNames = x509Certificate.getSubjectAlternativeNames();
        if (subjectAlternativeNames == null) {
            return Set.of();
        } else {
            return subjectAlternativeNames.stream()
                    .filter(list -> dnsType.equals(list.get(0)))
                    .map(list -> (String) list.get(0)).collect(Collectors.toUnmodifiableSet());
        }
    }
}
