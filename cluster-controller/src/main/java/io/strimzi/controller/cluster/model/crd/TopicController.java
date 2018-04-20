/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model.crd;

public class TopicController {
    private String image;
    private String watchedNamespace;
    private int reconciliationIntervalSeconds;
    private int zookeeperSessionTimeoutSeconds;
    private int topicMetadataMaxAttempts;

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public int getReconciliationIntervalSeconds() {
        return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(int reconciliationIntervalSeconds) {
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    public int getZookeeperSessionTimeoutSeconds() {
        return zookeeperSessionTimeoutSeconds;
    }

    public void setZookeeperSessionTimeoutSeconds(int zookeeperSessionTimeoutSeconds) {
        this.zookeeperSessionTimeoutSeconds = zookeeperSessionTimeoutSeconds;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }
}
