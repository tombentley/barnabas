/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Abstraction for things which know how to take a bunch of {@code TrustRoot TrustRoots}
 * and put them somewhere. This includes, but is not limited to, putting them in a Secret for consumption by
 * components needing to know what to trust.
 */
public interface TrustOutputWriter {
    void write(Collection<TrustRoot> trustRoot) throws CertificateEncodingException;
}
/**
 * A TrustOutputWriter impl supporting the legacy layout, suitable for
 * {@code $CLUSTER-cluster-ca-certs} and {@code $CLUSTER-client-ca-certs}.
 */
class LegacyTrustOutputWriter implements TrustOutputWriter {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral('-')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalStart()
            .appendOffsetId()
            .toFormatter();

    private final Resource<Secret> secretResource;

    public LegacyTrustOutputWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Collection<TrustRoot> trustRoot) throws CertificateEncodingException {
        List<TrustRoot> collect = trustRoot.stream().sorted(Comparator.comparing(TrustRoot::notAfter)).collect(Collectors.toList());
        Map<String, String> data = new HashMap<>();
        if (!collect.isEmpty()) {
            TrustRoot last = collect.remove(collect.size() - 1);
            data.put("ca.crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(last.x509())));
            for (TrustRoot root : collect) {
                X509Certificate x509 = root.x509();
                data.put("ca-"+ DATE_TIME_FORMATTER.format(x509.getNotAfter().toInstant().atZone(ZoneId.of("UTC"))) + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemString(x509).getBytes(StandardCharsets.ISO_8859_1)));
            }
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret))
                .withData(data).build());
    }
}
/**
 * A TrustOutputWriter impl supporting the new layout, suitable for
 * {@code $CLUSTER-$CA-trust}.
 */
class NewTrustOutputWriter implements TrustOutputWriter {

    private final Resource<Secret> secretResource;

    public NewTrustOutputWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Collection<TrustRoot> trustRoots) throws CertificateEncodingException {
        Map<String, String> data = new HashMap<>();
        for (TrustRoot root : trustRoots) {
            data.put(root.fingerprint() + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(root.x509())));
            // TODO data.put(root.fingerprint() + ".p12", Base64.getEncoder().encodeToString(PkiUtils.asP12(root.x509())));
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret)).withData(data).build());
    }
}
