/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
public class Storage {
    private String type;
    private String size;
    private String storageClass;
    private Map<String, String> selector;
    private boolean deleteClaim;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    @JsonProperty("class")
    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public Map<String, String> getSelector() {
        return selector;
    }

    public void setSelector(Map<String, String> selector) {
        this.selector = selector;
    }

    public boolean isDeleteClaim() {
        return deleteClaim;
    }

    public void setDeleteClaim(boolean deleteClaim) {
        this.deleteClaim = deleteClaim;
    }
}
