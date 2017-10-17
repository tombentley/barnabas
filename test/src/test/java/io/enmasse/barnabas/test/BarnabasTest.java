package io.enmasse.barnabas.test;

import io.fabric8.openshift.client.OpenShiftClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.fabric8.kubernetes.client.KubernetesClient;

import static io.fabric8.kubernetes.assertions.Assertions.assertThat;

@RunWith(Arquillian.class)
public class BarnabasTest {

    @ArquillianResource
    OpenShiftClient client;

    @Test
    public void testRunningPodStaysUp() throws Exception {
        assertThat(client).deployments().pods().isPodReadyForPeriod();
    }
}

