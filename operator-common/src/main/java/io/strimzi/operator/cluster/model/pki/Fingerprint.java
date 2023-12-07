/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.util.Arrays;
import java.util.Objects;

/**
 * A SHA-1 certificate fingerprint.
 *
 * The strength of the digest should not affect the security of the PKI because the
 * fingerprints are only used by the {@link CertificateController}
 * and {@link TrustController} to identify certificates and it is assumed that the user has taken appropriate steps
 * (such as RBAC on Secrets) to ensure the controllers are isolated from attackers seeking to introduce certificates
 * whose digests collide. A stronger digest could be used, but SHA-1 is convenient since it's what OpenSSL uses.
 */
public class Fingerprint implements Comparable<Fingerprint> {
    static final int BYTE_LENGTH = 20;
    static final int HEX_LENGTH = 40;
    private final byte[] bytes;

    public Fingerprint(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length != BYTE_LENGTH) {
            throw new IllegalArgumentException("Wrong length: " + bytes.length);
        }
        this.bytes = bytes;
    }

    private static int readHex(char c1) {
        if ('0' <= c1 && c1 <= '9') {
            return ((int) c1) - '0';
        } else if ('a' <= c1 && c1 <= 'f') {
            return 10 + ((int) c1) - 'a';
        } else if ('A' <= c1 && c1 <= 'F') {
            return 10 + ((int) c1) - 'A';
        } else {
            throw new IllegalArgumentException("Illegal hex character: " + c1);
        }
    }

    public Fingerprint(String hex) {
        if (hex.length() != HEX_LENGTH) {
            throw new IllegalArgumentException("Wrong number of characters in hex fingerprint: " + hex.length());
        }
        byte[] bytes = new byte[BYTE_LENGTH];
        for (int i = 0; i < HEX_LENGTH; i += 2) {
            int s1 = readHex(hex.charAt(i));
            int s2 = readHex(hex.charAt(i + 1));
            bytes[i / 2] = (byte) (s1 << 4 | s2);
        }
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fingerprint that = (Fingerprint) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return hex(bytes);
    }

    private static String hex(byte[] bytes) {
        char[] hexChars = "0123456789ABCDEF".toCharArray();
        char[] resultChars = new char[bytes.length * 2];
        for (int i = 0; i < resultChars.length; i += 2) {
            int v = bytes[i / 2] & 0xFF;
            resultChars[i] = hexChars[v >>> 4];
            resultChars[i + 1] = hexChars[v & 0x0F];
        }
        return new String(resultChars);
    }

    @Override
    public int compareTo(Fingerprint o) {
        return Arrays.compare(this.bytes, o.bytes);
    }
}
