/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;

public abstract class AbstractCrdIT extends BaseITST {

    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractCrdIT.class);

    @BeforeClass
    public static void logStart() {
        LOGGER.info("Starting test class");
    }

    @AfterClass
    public static void logEnd() {
        LOGGER.info("Finishing test class");
    }

    protected void assumeKube1_11Plus() {
        VersionInfo version = new DefaultKubernetesClient().getVersion();
        Assume.assumeTrue("1".equals(version.getMajor())
                && Integer.parseInt(version.getMinor().split("\\D")[0]) >= 11);
    }

    protected <T extends CustomResource> void createDelete(Class<T> resourceClass, String resource) {
        String ssStr = TestUtils.readResource(resourceClass, resource);
        assertNotNull("Class path resource " + resource + " was missing", ssStr);
        createDelete(ssStr);
        T model = TestUtils.fromYaml(resource, resourceClass, false);
        ssStr = TestUtils.toYamlString(model);
        try {
            createDelete(ssStr);
        } catch (Error | RuntimeException e) {
            System.err.println(ssStr);
            throw new AssertionError("Create delete failed after first round-trip -- maybe a problem with a defaulted value?", e);
        }
    }

    private void createDelete(String ssStr) {
        RuntimeException thrown = null;
        RuntimeException thrown2 = null;
        try {
            try {
                kubeCluster().cmdClient().applyContent(ssStr);
            } catch (RuntimeException t) {
                thrown = t;
            }
        } finally {
            try {
                kubeCluster().cmdClient().deleteContent(ssStr);
            } catch (RuntimeException t) {
                thrown2 = t;
            }
        }
        if (thrown != null) {
            if (thrown2 != null) {
                thrown.addSuppressed(thrown2);
            }
            throw thrown;
        } else if (thrown2 != null) {
            throw thrown2;
        }
    }

    @Before
    public void setupTests() {
        LOGGER.info("Setting up cluster");
        kubeCluster().before();
        LOGGER.info("Done setting up cluster");
    }
}
