/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class KafkaBrokerConfigurationDiffTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final String KAFKA_VERSION = "2.4.0";
    KafkaVersion kafkaVersion = VERSIONS.version(KAFKA_VERSION);
    private int brokerId = 0;

    private String getDesiredConfiguration(List<ConfigEntry> additional) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("desired-kafka-broker.conf")) {
            String desiredConfigString = TestUtils.readResource(is);

            for (ConfigEntry ce : additional) {
                desiredConfigString += "\n" + ce.name() + "=" + ce.value();
            }

            return desiredConfigString;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private Map<ConfigResource, Config> getCurrentConfiguration(List<ConfigEntry> additional) {
        Map<ConfigResource, Config> current = new HashMap<>();
        ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId));
        List<ConfigEntry> entryList = new ArrayList<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("current-kafka-broker.conf")) {

            List<String> configList = Arrays.asList(TestUtils.readResource(is).split(System.getProperty("line.separator")));
            configList.forEach(entry -> {
                String[] split = entry.split("=");
                String val = split.length == 1 ? "" : split[1];
                ConfigEntry ce = new ConfigEntry(split[0].replace("\n", ""), val, true, true, false);
                entryList.add(ce);
            });
            for (ConfigEntry ce : additional) {
                entryList.add(ce);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Config config = new Config(entryList);
        current.put(cr, config);
        return current;
    }

    @Test
    public void testDefaultValue() {
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()), getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.isDesiredPropertyDefaultValue("offset.metadata.max.bytes"), is(true));
    }

    @Test
    public void testNonDefaultValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("offset.metadata.max.bytes", "4097"));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.isDesiredPropertyDefaultValue("offset.metadata.max.bytes"), is(false));
    }

    @Test
    public void testCustomPropertyAdded() {
        ArrayList<ConfigEntry> ces = new ArrayList<>();
        ces.add(new ConfigEntry("custom.property", "42", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(new ArrayList<>()), getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().unknownConfigs(kafkaVersion).size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("custom.property", "42")), is(true));
    }

    @Test
    public void testCustomPropertyRemoved() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(emptyList()), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().unknownConfigs(kafkaVersion).size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("custom.property", "deleted entry")), is(true));
    }

    @Test
    public void testCustomPropertyKept() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().unknownConfigs(kafkaVersion).size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testCustomPropertyChanged() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("custom.property", "42", false, true, false));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("custom.property", "43", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().unknownConfigs(kafkaVersion).size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("custom.property", "43")), is(true));
    }

    @Test
    public void testChangedPresentValue() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "2", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("min.insync.replicas", "2")), is(true));
    }

    @Test
    public void testChangedPresentValueToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("min.insync.replicas", "1", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedAdvertisedListener() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedAdvertisedListenerFromNothingToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedAdvertisedListenerFromNonDefaultToDefault() {
        // advertised listeners are filled after the pod started
        List<ConfigEntry> ces = singletonList(new ConfigEntry("advertised.listeners", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedZookeeperConnect() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("zookeeper.connect", "karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "/var/lib/kafka/data/karel", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("log.dirs", "/var/lib/kafka/data/karel")), is(true));
    }

    @Test
    public void testLogDirsNonDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("log.dirs", "null")), is(true));
    }

    @Test
    public void testLogDirsDefaultToDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testUnchangedLogDirs() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("log.dirs", "null", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(0));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedInterBrokerListenerName() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("inter.broker.listener.name", "david", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
    }

    @Test
    public void testChangedListenerSecurityProtocolMap() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "david", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
    }

    @Test
    public void testChangedListenerSecurityProtocolMapFromNonDefault() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL,EXTERNAL-9094:SSL", false, true, false));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(false));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("listener.security.protocol.map", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL")), is(true));
    }

    @Test
    public void testListenerChanged() {
        List<ConfigEntry> ces = singletonList(new ConfigEntry("listener", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL,EXTERNAL-9094:SSL", false, true, false));
        List<ConfigEntry> ces2 = singletonList(new ConfigEntry("listener", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(ces),
                getDesiredConfiguration(ces2), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(1));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
        assertThat(kcd.getDiff().asOrderedProperties().asMap().equals(singletonMap("listener", "REPLICATION-9091:SSL,PLAIN-9092:SASL_PLAINTEXT,TLS-9093:SSL")), is(true));
    }

    @Test
    public void testChangedMoreProperties() {
        ArrayList<ConfigEntry> ces = new ArrayList<>();
        ces.add(new ConfigEntry("inter.broker.listener.name", "david", false, true, false));
        ces.add(new ConfigEntry("inter.broker.listener.name2", "karel", false, true, false));
        ces.add(new ConfigEntry("inter.broker.listener.name3", "honza", false, true, false));
        KafkaBrokerConfigurationDiff kcd = new KafkaBrokerConfigurationDiff(getCurrentConfiguration(emptyList()),
                getDesiredConfiguration(ces), kafkaVersion, brokerId);
        assertThat(kcd.getDiff().asOrderedProperties().asMap().size(), is(3));
        assertThat(kcd.cannotBeUpdatedDynamically(), is(true));
    }

}
