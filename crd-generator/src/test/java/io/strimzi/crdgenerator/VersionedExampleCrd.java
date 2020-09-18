/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.PresentInVersions;

@Crd(
        spec = @Crd.Spec(
                group = "crdgenerator.strimzi.io",
                names = @Crd.Spec.Names(
                        kind = "Example",
                        plural = "examples",
                        categories = {"strimzi"}),
                scope = "Namespaced",
                versions = {
                        @Crd.Spec.Version(name = "v1", served = true, storage = true),
                        @Crd.Spec.Version(name = "v2", served = true, storage = false)
                },
                subresources = @Crd.Spec.Subresources(
                       status = {@Crd.Spec.Subresources.Status()},
                       scale = {
                               @Crd.Spec.Subresources.Scale(
                                   apiVersion = "v1",
                                   labelSelectorPath = "v1.dsdvc",
                                   specReplicasPath = "v1.dcsdvsv",
                                   statusReplicasPath = "v1.sdvsdvs"),
                               @Crd.Spec.Subresources.Scale(
                                   apiVersion = "v2",
                                   labelSelectorPath = "v2.ssdv",
                                   specReplicasPath = "v2.dcsdvsv",
                                   statusReplicasPath = "v2.sdvsdvs")
                       }
                ),
                additionalPrinterColumns = {
                        @Crd.Spec.AdditionalPrinterColumn(
                                apiVersion = "v1",
                                name = "V1 column",
                                description = "The foo",
                                jsonPath = "...",
                                type = "integer"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                apiVersion = "v2",
                                name = "V2 column",
                                description = "The bar",
                                jsonPath = "...",
                                type = "integer"
                        )
                }
        ))
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
public class VersionedExampleCrd<T, U extends Number, V extends U> extends CustomResource {

    @Description(apiVersions = "v1", value = "V1 description")
    @Description(apiVersions = "v2", value = "V2 description")
    @Pattern(apiVersions = "v1", value = "v1Pattern")
    @Pattern(apiVersions = "v2", value = "v2Pattern")
    public String ignored;

    @Minimum(apiVersions = "v1", value = 0)
    @Minimum(apiVersions = "v2", value = 4)
    @Maximum(apiVersions = "v1", value = 10)
    @Maximum(apiVersions = "v2", value = 12)
    public int someInt;

    @Minimum(apiVersions = "v2+", value = 4)
    @Maximum(value = 10)
    public int someOtherInt;

    @PresentInVersions("v1")
    public String removed;

    @PresentInVersions("v2+")
    public String added;

    public VersionedMapOrList typeChange;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedExampleCrd<?, ?, ?> that = (VersionedExampleCrd<?, ?, ?>) o;
        return someInt == that.someInt &&
                someOtherInt == that.someOtherInt &&
                Objects.equals(ignored, that.ignored) &&
                Objects.equals(removed, that.removed) &&
                Objects.equals(added, that.added) &&
                Objects.equals(typeChange, that.typeChange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignored, someInt, someOtherInt, removed, added, typeChange);
    }
}
