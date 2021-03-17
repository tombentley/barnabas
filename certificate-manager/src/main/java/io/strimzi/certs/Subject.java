/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the subject for a certificate.
 * Can be serialized as JSON.
 */
public class Subject {

    public static class Builder {
        private String organizationName;
        private String commonName;
        private Set<String> dnsNames = null;
        private Set<String> ipAddresses = null;

        public Builder withCommonName(String commonName) {
            this.commonName = commonName;
            return this;
        }
        public Builder withOrganizationName(String organizationName) {
            this.organizationName = organizationName;
            return this;
        }
        public Builder addDnsName(String dnsName) {
            if (dnsNames == null) {
                 dnsNames = new HashSet<>();
            }
            dnsNames.add(dnsName);
            return this;
        }
        public Builder addIpAddress(String ip) {
            if (ipAddresses == null) {
                ipAddresses = new HashSet<>();
            }
            ipAddresses.add(ip);
            return this;
        }
        public Subject build() {
            return new Subject(commonName, organizationName, dnsNames, ipAddresses);
        }

    }

    private final String organizationName;
    private final String commonName;
    private final Set<String> dnsNames;
    private final Set<String> ipAddresses;

    @JsonCreator
    private Subject(
            @JsonProperty String commonName,
            @JsonProperty String organizationName,
            @JsonProperty Set<String> dnsNames,
            @JsonProperty Set<String> ipAddresses) {
        this.organizationName = organizationName;
        this.commonName = commonName;
        this.dnsNames = Collections.unmodifiableSet(dnsNames);
        this.ipAddresses = Collections.unmodifiableSet(ipAddresses);
    }

    @JsonProperty
    public String organizationName() {
        return organizationName;
    }

    @JsonProperty
    public String commonName() {
        return commonName;
    }

    @JsonProperty
    public Set<String> dnsNames() {
        return dnsNames;
    }

    @JsonProperty
    public Set<String> ipAddresses() {
        return ipAddresses;
    }

    public Map<String, String> subjectAltNames() {
        Map<String, String> san = new HashMap<>();
        int i = 0;
        for (String name : dnsNames()) {
            san.put("DNS." + (i++), name);
        }
        i = 0;
        for (String ip : ipAddresses()) {
            san.put("IP." + (i++), ip);
        }
        return san;
    }

    /**
     * @return The DN in the format understood by {@code openssl}.
     */
    public String opensslDn() {
        StringBuilder bldr = new StringBuilder();

        if (organizationName != null)   {
            bldr.append(String.format("/O=%s", organizationName));
        }

        if (commonName != null)   {
            bldr.append(String.format("/CN=%s", commonName));
        }

        return bldr.toString();
    }
}
