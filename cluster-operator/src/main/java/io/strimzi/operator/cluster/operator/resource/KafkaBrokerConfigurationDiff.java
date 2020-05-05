/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

/**
 * Computes a diff between the current config (supplied as a Map ConfigResource) and the desired config (supplied as a String).
 * An algorithm:
 *  1. Create map from supplied desired String
 *  2. Fill placeholders (e.g. ${BROKER_ID}) in desired map
 *  3a. Loop over all entries. If the entry is in IGNORABLE_PROPERTIES or entry.value from desired is equal to entry.value from current, do nothing
 *      else add it to the diff
 *  3b. If entry was removed from desired, add it to the diff with null value.
 *  3c. If custom entry was removed, delete property
 *
 */
public class KafkaBrokerConfigurationDiff extends AbstractResourceDiff {

    private static final Logger log = LogManager.getLogger(KafkaBrokerConfigurationDiff.class);
    private final Map<ConfigResource, Config> current;
    private Collection<ConfigEntry> currentEntries;
    private String desired;
    private KafkaConfiguration diff;
    private KafkaVersion kafkaVersion;
    private int brokerId;
    Map<ConfigResource, Collection<AlterConfigOp>> updated = new HashMap<>();

    /**
     * These options are skipped because they contain placeholders
     * 909[1-4] is for skipping all (internal, plain, secured, external) listeners properties
     */
    public static final Pattern IGNORABLE_PROPERTIES = Pattern.compile(
            "^(broker\\.id"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.location"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.password"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.type"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.location"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.password"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.type"
            + "|.*-909[1-4]\\.ssl\\.client\\.auth"
            + "|.*-909[1-4]\\.scram-sha-512\\.sasl\\.jaas\\.config"
            + "|.*-909[1-4]\\.sasl\\.enabled\\.mechanisms"
            + "|advertised\\.listeners"
            + "|zookeeper\\.connect"
            + "|super\\.users"
            + "|broker\\.rack)$");

    public KafkaBrokerConfigurationDiff(Map<ConfigResource, Config> current, String desired, KafkaVersion kafkaVersion, int brokerId) {
        this.current = current;
        this.diff = emptyKafkaConf(); // init
        this.desired = desired;
        this.kafkaVersion = kafkaVersion;
        this.brokerId = brokerId;
        Config brokerConfigs = this.current.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)));
        if (brokerConfigs != null) {
            this.currentEntries = brokerConfigs.entries();
            this.diff = computeDiff();
        }
    }

    private KafkaConfiguration emptyKafkaConf() {
        return new KafkaConfiguration(Collections.emptySet());
    }


    private void fillPlaceholderValue(Map<String, String> orderedProperties, String placeholder, String value) {
        orderedProperties.entrySet().forEach(entry -> {
            entry.setValue(entry.getValue().replaceAll("\\$\\{" + Pattern.quote(placeholder) + "\\}", value));
        });
    }

    /**
     * Returns computed diff
     * @return computed diff
     */
    public KafkaConfiguration getDiff() {
        return this.diff;
    }

    /**
     * Returns true if property in desired map has a default value
     * @param key name of the property
     * @return true if property in desired map has a default value
     */
    public boolean isDesiredPropertyDefaultValue(String key) {
        Optional<ConfigEntry> entry = currentEntries.stream().filter(configEntry -> configEntry.name().equals(key)).findFirst();
        if (entry.isPresent()) {
            return entry.get().isDefault();
        }
        return false;
    }

    /* test */public boolean cannotBeUpdatedDynamically() {
        if (diff == null) {
            return false;
        } else return diff.anyReadOnly(kafkaVersion)
                || !diff.unknownConfigs(kafkaVersion).isEmpty()
                || diff.containsListenersChange();
    }

    /**
     * @return Map object which is used for dynamic configuration of kafka broker
     */
    public Map<ConfigResource, Collection<AlterConfigOp>> getUpdatedConfig() {
        return updated;
    }

    private boolean isIgnorableProperty(String key) {
        return IGNORABLE_PROPERTIES.matcher(key).matches();
    }

    /**
     * Computes diff between two maps. Entries in IGNORABLE_PROPERTIES are skipped
     * @return KafkaConfiguration containing all entries which were changed from current in desired configuration
     */
    public KafkaConfiguration computeDiff() {
        Map<String, String> currentMap;
        Map<String, String> difference = new HashMap<>();

        Collection<AlterConfigOp> updatedCE = new ArrayList<>();

        currentMap = currentEntries.stream().collect(Collectors.toMap(configEntry -> configEntry.name(), configEntry -> configEntry.value() == null ? "null" : configEntry.value()));

        Map<String, String> desiredMap = ModelUtils.stringToMap(desired);

        fillPlaceholderValue(desiredMap, "STRIMZI_BROKER_ID", Integer.toString(brokerId));

        JsonNode source = patchMapper().valueToTree(currentMap);
        JsonNode target = patchMapper().valueToTree(desiredMap);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode d : jsonDiff) {
            String pathValue = d.get("path").asText();
            String pathValueWithoutSlash = pathValue.substring(1);

            Optional<ConfigEntry> optEntry = currentEntries.stream().filter(configEntry -> configEntry.name().equals(pathValueWithoutSlash)).findFirst();

            if (optEntry.isPresent()) {
                ConfigEntry entry = optEntry.get();
                if (d.get("op").asText().equals("remove")) {
                    if (!entry.source().equals(ConfigEntry.ConfigSource.DEFAULT_CONFIG)) {
                        // we are deleting custom option
                        difference.put(pathValueWithoutSlash, "deleted entry");
                        updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, entry.value()), AlterConfigOp.OpType.DELETE));
                        log.trace("removing custom property {}", entry.name());
                    } else if (entry.isDefault()) {
                        // entry is in current, is not in desired, is default -> it uses default value, skip
                        log.trace("{} not set in desired, using default value", entry.name());
                    } else {
                        // entry is in current, is not in desired, is not default -> it was using non-default value and was removed
                        // if the entry was custom, it should be deleted
                        if (!isIgnorableProperty(pathValueWithoutSlash)) {
                            difference.put(pathValueWithoutSlash, null);
                            updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, null), AlterConfigOp.OpType.DELETE));
                            log.trace("{} not set in desired, unsetting back to default {}", entry.name(), null);
                        } else {
                            log.trace("{} is ignorable, not considering as removed");
                        }
                    }
                } else if (d.get("op").asText().equals("replace")) {
                    // entry is in the current, desired is updated value
                    if (!isIgnorableProperty(pathValueWithoutSlash)) {
                        log.trace("{} has new desired value {}", entry.name(), desiredMap.get(entry.name()));
                        updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash)), AlterConfigOp.OpType.SET));
                        difference.put(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash));
                    } else {
                        log.trace("{} is ignorable, not considering as replaced");
                    }
                }
            } else {
                if (d.get("op").asText().equals("add")) {
                    // entry is not in the current, it is added
                    if (!isIgnorableProperty(pathValueWithoutSlash)) {
                        log.trace("add new {} {}", pathValueWithoutSlash, d.get("op").asText());
                        difference.put(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash));
                        updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash)), AlterConfigOp.OpType.SET));
                    } else {
                        log.trace("{} is ignorable, not considering as added");
                    }
                }
            }

            /*log.debug("Kafka Broker Config Differs : {}", d);
            log.debug("Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
            log.debug("Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));*/
        }


        difference.entrySet().forEach(e -> {
            log.info("{} broker conf differs: '{}' -> '{}'", e.getKey(), currentMap.get(e.getKey()), e.getValue());
        });

        updated.put(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)), updatedCE);
        return KafkaConfiguration.unvalidated(difference);
    }

    @Override
    public boolean isEmpty() {
        return diff.isEmpty();
    }
}
