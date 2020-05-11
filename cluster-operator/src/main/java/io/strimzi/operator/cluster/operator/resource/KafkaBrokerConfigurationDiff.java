/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
    private Map<ConfigResource, Collection<AlterConfigOp>> diff;
    private KafkaVersion kafkaVersion;
    private int brokerId;
    private Map<String, ConfigModel> configModel;

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
            + "|broker\\.rack)$");

    public KafkaBrokerConfigurationDiff(Map<ConfigResource, Config> current, String desired, KafkaVersion kafkaVersion, int brokerId) {
        this.current = current;
        this.diff = Collections.emptyMap(); // init
        this.desired = desired;
        this.kafkaVersion = kafkaVersion;
        this.configModel = KafkaConfiguration.readConfigModel(kafkaVersion);
        this.brokerId = brokerId;
        Config brokerConfigs = this.current.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)));
        if (brokerConfigs != null) {
            this.currentEntries = brokerConfigs.entries();
            this.diff = computeDiff();
        }
    }

    private void fillPlaceholderValue(Map<String, String> orderedProperties, String placeholder, String value) {
        orderedProperties.entrySet().forEach(entry -> {
            entry.setValue(entry.getValue().replaceAll("\\$\\{" + Pattern.quote(placeholder) + "\\}", value));
        });
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

    public boolean canBeUpdatedDynamically() {
        if (diff == null) {
            return true;
        } else {
            AtomicBoolean tempResult = new AtomicBoolean(true);
            diff.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId))).forEach(entry -> {
                if (isEntryReadOnly(entry.configEntry())) {
                    tempResult.set(false);
                }
            });
            return tempResult.get();
        }
    }

    /**
     * @param entry tested ConfigEntry
     * @return true if the entry is READ_ONLY
     */
    private boolean isEntryReadOnly(ConfigEntry entry) {
        return configModel.get(entry.name()).getScope().equals(Scope.READ_ONLY);
    }

    /**
     * @return Map object which is used for dynamic configuration of kafka broker
     */
    public Map<ConfigResource, Collection<AlterConfigOp>> getConfigDiff() {
        return diff;
    }

    /**
     *
     * @return size of the broker config difference
     */
    public int getDiffSize() {
        return diff.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId))).size();
    }

    private boolean isIgnorableProperty(String key) {
        return IGNORABLE_PROPERTIES.matcher(key).matches();
    }

    /**
     * Computes diff between two maps. Entries in IGNORABLE_PROPERTIES are skipped
     * @return KafkaConfiguration containing all entries which were changed from current in desired configuration
     */
    public Map<ConfigResource, Collection<AlterConfigOp>> computeDiff() {
        Map<ConfigResource, Collection<AlterConfigOp>> updated = new HashMap<>();
        Map<String, String> currentMap;

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
                    if (isEntryCustom(entry)) {
                        // we are deleting custom option
                        //updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, entry.value()), AlterConfigOp.OpType.DELETE));
                        log.trace("removing custom property {}", entry.name());
                    } else if (entry.isDefault()) {
                        // entry is in current, is not in desired, is default -> it uses default value, skip.
                        // Some default properties do not have set ConfigEntry.ConfigSource.DEFAULT_CONFIG and thus
                        // we are removing property. That might cause redundant RU. To fix this we would have to add defaultValue
                        // to the configModel
                        log.trace("{} not set in desired, using default value", entry.name());
                    } else {
                        // entry is in current, is not in desired, is not default -> it was using non-default value and was removed
                        // if the entry was custom, it should be deleted
                        if (!isIgnorableProperty(pathValueWithoutSlash)) {
                            updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, null), AlterConfigOp.OpType.DELETE));
                            log.trace("{} not set in desired, unsetting back to default {}", entry.name(), "deleted entry");
                        } else {
                            log.trace("{} is ignorable, not considering as removed");
                        }
                    }
                } else if (d.get("op").asText().equals("replace")) {
                    // entry is in the current, desired is updated value
                    if (!isIgnorableProperty(pathValueWithoutSlash)) {
                        if (isEntryCustom(entry)) {
                            log.trace("custom property {} has new desired value {}", entry.name(), desiredMap.get(entry.name()));
                        } else {
                            log.trace("property {} has new desired value {}", entry.name(), desiredMap.get(entry.name()));
                            updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash)), AlterConfigOp.OpType.SET));
                        }
                    } else {
                        log.trace("{} is ignorable, not considering as replaced");
                    }
                }
            } else {
                if (d.get("op").asText().equals("add")) {
                    // entry is not in the current, it is added
                    if (!isIgnorableProperty(pathValueWithoutSlash)) {
                        if (isEntryCustom(pathValueWithoutSlash)) {
                            log.trace("add new custom property {} {}", pathValueWithoutSlash, d.get("op").asText());
                        } else {
                            log.trace("add new {} {}", pathValueWithoutSlash, d.get("op").asText());
                            updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, desiredMap.get(pathValueWithoutSlash)), AlterConfigOp.OpType.SET));
                        }
                    } else {
                        log.trace("{} is ignorable, not considering as added");
                    }
                }
            }

            log.debug("Kafka Broker {} Config Differs : {}", brokerId, d);
            log.debug("Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
            log.debug("Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
        }

        updated.put(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId)), updatedCE);
        return updated;
    }

    @Override
    public boolean isEmpty() {
        return diff.isEmpty();
    }

    /**
     * For some reason not all default entries have set ConfigEntry.ConfigSource.DEFAULT_CONFIG so we need to compare
     * @param entry tested ConfigEntry
     * @return true if entry is default (not custom)
     */
    private boolean isEntryCustom(ConfigEntry entry) {
        return isEntryCustom(entry.name());
    }

    private boolean isEntryCustom(String entryName) {
        return !this.configModel.keySet().contains(entryName);
    }

}
