/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Represents a status of the Kafka resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "listeners" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaStatus extends Status {
    private static final long serialVersionUID = 1L;

    private List<ListenerStatus> listeners;
    
    private String clusterId;

    private List<CaCertStatus> clusterCa;

    private List<CaCertStatus> clientsCa;

    @Description("Addresses of the internal and external listeners")
    public List<ListenerStatus> getListeners() {
        return listeners;
    }

    public void setListeners(List<ListenerStatus> listeners) {
        this.listeners = listeners;
    }
    
    @Description("Kafka cluster Id")
    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Description("The state of CA certificates used for the \"Cluster CA\".")
    public List<CaCertStatus> getClusterCa() {
        return clusterCa;
    }

    public void setClusterCa(List<CaCertStatus> clusterCa) {
        this.clusterCa = clusterCa;
    }

    @Description("The state of CA certificates used for the \"Clients CA\".")
    public List<CaCertStatus> getClientsCa() {
        return clientsCa;
    }

    public void setClientsCa(List<CaCertStatus> clientsCa) {
        this.clientsCa = clientsCa;
    }
}
