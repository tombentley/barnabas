/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;

import static io.strimzi.operator.cluster.model.pki.TrustManager.fingerprint;

public class IssuedCertificate {
    private final byte[] key;
    private final X509Certificate issuedCertificate;
    private final List<X509Certificate> intermediateChain;
    private final X509Certificate rootCaCertificate;

    public IssuedCertificate(byte[] key,
                             X509Certificate issuedCertificate,
                             List<X509Certificate> intermediateChain,
                             X509Certificate rootCaCertificate) {
        this.key = key;
        this.issuedCertificate = issuedCertificate;
        this.intermediateChain = intermediateChain;
        this.rootCaCertificate = rootCaCertificate;
    }

    /**
     * @return The private key corresponding to the certificate that was issued.
     */
    public byte[] privateKey() {
        return key;
    }

    /**
     * @return The certificate that was issued.
     */
    public X509Certificate issuedCertificate() {
        return issuedCertificate;
    }

    /**
     * The certificate chain for all the intermediate CA certificates between the
     * {@linkplain #issuedCertificate() issued certificate} and the
     * {@linkplain #rootCaCertificate()} root CA certificate
     * @return The certificate chain for all the intermediate CA certificates.
     */
    public List<X509Certificate> intermediateChain() {
        return intermediateChain;
    }

    /**
     * The complete chain of certificates including both the {@link #issuedCertificate()}
     * and the {@link #intermediateChain()}.
     * @return The complete chain of certificates
     */
    public List<X509Certificate> completeChain() {
        List<X509Certificate> result = new ArrayList<>(intermediateChain().size() + 1);
        result.add(issuedCertificate());
        result.addAll(intermediateChain());
        return result;
    }

    /**
     * The certificate of the root CA.
     * This has to be added to the trust stores of relying parties.
     * @return The certificate of the root CA.
     */
    public X509Certificate rootCaCertificate() {
        return rootCaCertificate;
    }

    /**
     * Persist the issued key, certificate and intermediate chain as PKCS12 and return the bytes.
     * Does not persist the root certificate.
     * @param password The password for protecting the both the key and the whole PKCS12 data.
     * @return The PKCS12 bytes.
     * @throws KeyStoreException If the keystore could not be constructed.
     * @throws IOException IO problems.
     * @throws NoSuchAlgorithmException If an unsupported algorithm was used.
     * @throws CertificateException Something wrong with the certificate.
     * @throws InvalidKeySpecException Bad keyspec.
     */
    public byte[] toPkcs12(char[] password) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, InvalidKeySpecException {
        IssuedCertificate issuedCert = this;
        String pemPrivateKey = new String(issuedCert.privateKey(), StandardCharsets.US_ASCII)
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "");

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        List<X509Certificate> certificateChain = issuedCert.completeChain();
        String alias = fingerprint(issuedCert.issuedCertificate()).toString();
        ks.setEntry(alias,
                new KeyStore.PrivateKeyEntry(
                        // TODO is it always RSA even for EC crypto?
                        KeyFactory.getInstance("RSA")
                                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pemPrivateKey))),
                        certificateChain.toArray(Certificate[]::new)),
                new KeyStore.PasswordProtection(password));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ks.store(baos, password);
        return baos.toByteArray();
    }

    /**
     * Restores a IssuedCertificate previously stored using {@link #toPkcs12(char[])}.
     * @param bytes The PKCS12 bytes.
     * @param password The password used when storing.
     * @return The IssuedCertificate, with a null root CA certificate.
     * @throws KeyStoreException If the keystore could not be constructed.
     * @throws IOException IO problems.
     * @throws NoSuchAlgorithmException If an unsupported algorithm was used.
     * @throws CertificateException Something wrong with the certificate.
     * @throws UnrecoverableEntryException If the entry could not be read.
     */
    public static IssuedCertificate fromPkcs12(byte[] bytes, char[] password) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableEntryException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream is = new ByteArrayInputStream(bytes)) {
            ks.load(is, password);
        }
        Enumeration<String> aliases = ks.aliases();
        if (!aliases.hasMoreElements()) {
            throw new RuntimeException("Empty keystore");
        }
        String alias = aliases.nextElement();
        if (aliases.hasMoreElements()) {
            throw new RuntimeException("Unexpected alias");
        }
        KeyStore.Entry entry = ks.getEntry(alias, new KeyStore.PasswordProtection(password));

        // TODO check it's just these aliases

        if (entry instanceof KeyStore.PrivateKeyEntry) {
            KeyStore.PrivateKeyEntry pkentry = (KeyStore.PrivateKeyEntry) entry;
            @SuppressWarnings("unchecked")
            List<X509Certificate> certificates = (List) Arrays.asList(pkentry.getCertificateChain());
            // TODO eugh
            byte[] encode = Base64.getEncoder().encode(pkentry.getPrivateKey().getEncoded());
            byte[] b = ("-----BEGIN PRIVATE KEY-----\n" + new String(encode) + "\n-----END PRIVATE KEY-----").getBytes();
            return new IssuedCertificate(b, certificates.get(0), certificates.subList(1, certificates.size()), null);
        } else {
            throw new RuntimeException("Entry is not a key");
        }
    }
}
