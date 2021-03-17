/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SubjectTests {
    @Test
    public void testSubjectToString()   {
        Subject.Builder sbj = new Subject.Builder()
            .withCommonName("joe");
        assertThat(sbj.build().opensslDn(), is("/CN=joe"));

        sbj = new Subject.Builder()
            .withOrganizationName("MyOrg");
        assertThat(sbj.build().opensslDn(), is("/O=MyOrg"));

        sbj = new Subject.Builder()
            .withCommonName("joe")
            .withOrganizationName("MyOrg");
        assertThat(sbj.build().opensslDn(), is("/O=MyOrg/CN=joe"));
    }
}
