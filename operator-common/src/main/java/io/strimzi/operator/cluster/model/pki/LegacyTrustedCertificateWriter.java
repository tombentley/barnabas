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
 * A TrustOutputWriter impl supporting the legacy layout, suitable for
 * {@code $CLUSTER-cluster-ca-certs} and {@code $CLUSTER-client-ca-certs}.
 */
public class LegacyTrustedCertificateWriter implements TrustedCertificateWriter {

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

    public LegacyTrustedCertificateWriter(Resource<Secret> secretResource) {
        this.secretResource = secretResource;
    }

    @Override
    public void write(Map<Fingerprint, X509Certificate> trustRoots) throws CertificateEncodingException {
        List<X509Certificate> collect = trustRoots.values().stream().sorted(Comparator.comparing(cert -> cert.getNotAfter().toInstant())).collect(Collectors.toList());
        Map<String, String> data = new HashMap<>();
        if (!collect.isEmpty()) {
            X509Certificate last = collect.remove(collect.size() - 1);
            data.put("ca.crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(last)));
            for (X509Certificate root : collect) {
                data.put("ca-" + DATE_TIME_FORMATTER.format(root.getNotAfter().toInstant().atZone(ZoneId.of("UTC"))) + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemString(root).getBytes(StandardCharsets.ISO_8859_1)));
            }
        }
        Secret secret = secretResource.get();
        secretResource.createOrReplace((secret == null ? new SecretBuilder() : new SecretBuilder(secret))
                .withData(data).build());
    }
}
