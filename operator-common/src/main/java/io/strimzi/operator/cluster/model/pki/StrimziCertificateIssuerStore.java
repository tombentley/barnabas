/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.pki;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <pre><code>
 * kind: Secret
 * metadata:
 *   name: $CLUSTER-$CA
 *   ownerRef: ...
 * data:
 *   ca.key
 *   ca.crt
 *   $FINGERPRINT1.key # optional
 *   $FINGERPRINT1.crt
 *   $FINGERPRINT2.key # optional
 *   $FINGERPRINT2.crt
 * </code></pre>
 */
public class StrimziCertificateIssuerStore {

    private static final Logger LOGGER = LogManager.getLogger(StrimziCertificateIssuerStore.class);

    private final Resource<Secret> caSecretResource;
    private final Clock clock;

    public StrimziCertificateIssuerStore(Clock clock, Resource<Secret> caSecretResource) {
        this.clock = Objects.requireNonNull(clock);
        this.caSecretResource = Objects.requireNonNull(caSecretResource);
    }

    public InternalCertificateIssuer bootstrapCa(Kafka kafkaAssembly) throws CertificateException {
        /*
         * So we only need a CA when we come to issue a cert, so is there a CertificateIssuer which defers the bootstrapping till then?
         * If we always just create one up front then we have to check here whether its still provides a valid cert path
         * which is extra certpath validation
         */
        CertificateAuthority certificateAuthority = kafkaAssembly == null || kafkaAssembly.getSpec() == null ?
                new CertificateAuthority() :
                kafkaAssembly.getSpec().getClusterCa();
        OwnerReference ownerRef;
        if (certificateAuthority.isGenerateSecretOwnerReference()) {
            ownerRef = new OwnerReferenceBuilder()
                    .withApiVersion(kafkaAssembly.getApiVersion())
                    .withKind(kafkaAssembly.getKind())
                    .withName(kafkaAssembly.getMetadata().getName())
                    .withUid(kafkaAssembly.getMetadata().getUid())
                    .withBlockOwnerDeletion(false)
                    .withController(false)
                    .build();
        } else {
            ownerRef = null;
        }
        Secret secret = caSecretResource.get();

        InternalCertificateIssuer issuer;
        if (secret != null && secret.getData() != null && secret.getData().containsKey("ca.key")) {
            issuer = loadIssuer(certificateAuthority, ownerRef, secret);
        } else {
            issuer = createIssuer(certificateAuthority, ownerRef);
        }
        return issuer;
    }

    private InternalCertificateIssuer loadIssuer(CertificateAuthority certificateAuthority, OwnerReference ownerRef, Secret secret) throws CertificateException {
        Map<String, String> data = secret.getData();
        // Check
        // TODO restore from Secrets
        String issuingKeyStr = data.remove("ca.key");
        String issuingCertStr = data.remove("ca.crt");
        if (issuingCertStr == null) {
            // TODO load it from the ca-certs secret
        }
        X509Certificate issuingCert = PkiUtils.x509Certificate(Base64.getDecoder().decode(issuingCertStr));
        byte[] issuingKey = Base64.getDecoder().decode(issuingKeyStr);
        List<X509Certificate> pathToRoot = new ArrayList<>(data.size() / 2);
        Pattern p = Pattern.compile("(?<fingerprint>[A-Z0-9]{40})\\.crt");
        for (var entry : data.entrySet()) {
            Matcher matcher = p.matcher(entry.getKey());
            if (matcher.matches()) {
                Fingerprint fingerprint = new Fingerprint(matcher.group("fingerprint"));
                String caCertStr = entry.getValue();
                String caKeyStr = data.get(fingerprint + ".key");
                byte[] caKey = Base64.getDecoder().decode(caKeyStr);
                // TODO if caKey is null it's not a problem UNLESS we need to sign a CA cert
                // TODO but what should we do in that case?
                X509Certificate caCrt = PkiUtils.x509Certificate(Base64.getDecoder().decode(caCertStr));
                pathToRoot.add(caCrt);
            } else {
                LOGGER.info("Unexpected entry in secret: entry: {}, secret: {}", entry.getKey(), secret.getMetadata().getName());
            }
        }
        pathToRoot.add(issuingCert);
        return new InternalCertificateIssuer(clock, new OpenSslCertManager(), pathToRoot, issuingKey, issuingCert);
    }

    private InternalCertificateIssuer createIssuer(CertificateAuthority certificateAuthority, OwnerReference ownerRef) throws CertificateEncodingException {
        int caKeyGeneration = 0; // TODO caKeySecret != null ? Annotations.intAnnotation(caKeySecret, ANNO_STRIMZI_IO_CA_KEY_GENERATION, INIT_GENERATION) : INIT_GENERATION;
        // TODO we also need to increment the generation based on the renewal policy
        Subject caSubject = new Subject.Builder()
                // Key replacements does not work if both old and new CA certs have the same subject DN, so include the
                // key generation in the DN so the certificates appear distinct during CA key replacement.
                .withCommonName("cluster-ca" + " v" + caKeyGeneration)
                .withOrganizationName("io.strimzi").build();

        int validityDays = certificateAuthority.getValidityDays();
        if (validityDays <= 0) {
            validityDays = CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS;
        }
        InternalCertificateIssuer issuer = InternalCertificateIssuer.createRootIssuer(clock, new OpenSslCertManager(), caSubject, validityDays);
        // store in Secrets
        SecretBuilder secretBuilder = new SecretBuilder();
        if (ownerRef != null) {
            secretBuilder.withNewMetadata()
                    .withOwnerReferences(ownerRef)
                    .endMetadata();
        }
        secretBuilder
                .addToData("ca.key", Base64.getEncoder().encodeToString(issuer.issuingKey()))
                .addToData("ca.crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(issuer.issuingCert())));
        for (var cert : issuer.chain()) {
            Fingerprint fp = PkiUtils.fingerprint(cert);
            secretBuilder
                    .addToData(fp + ".crt", Base64.getEncoder().encodeToString(PkiUtils.asPemBytes(issuer.issuingCert())));
        }
        caSecretResource.createOrReplace(secretBuilder
                .build());
        return issuer;
    }
}
