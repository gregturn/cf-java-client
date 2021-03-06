/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cloudfoundry.spring.client.v2.spacequotadefinitions;

import lombok.ToString;
import org.cloudfoundry.client.v2.spacequotadefinitions.AssociateSpaceQuotaDefinitionRequest;
import org.cloudfoundry.client.v2.spacequotadefinitions.AssociateSpaceQuotaDefinitionResponse;
import org.cloudfoundry.client.v2.spacequotadefinitions.GetSpaceQuotaDefinitionRequest;
import org.cloudfoundry.client.v2.spacequotadefinitions.GetSpaceQuotaDefinitionResponse;
import org.cloudfoundry.client.v2.spacequotadefinitions.ListSpaceQuotaDefinitionsRequest;
import org.cloudfoundry.client.v2.spacequotadefinitions.ListSpaceQuotaDefinitionsResponse;
import org.cloudfoundry.client.v2.spacequotadefinitions.RemoveSpaceQuotaDefinitionRequest;
import org.cloudfoundry.client.v2.spacequotadefinitions.SpaceQuotaDefinitions;
import org.cloudfoundry.client.v2.spaces.Spaces;
import org.cloudfoundry.spring.util.AbstractSpringOperations;
import org.cloudfoundry.spring.util.QueryBuilder;
import org.springframework.web.client.RestOperations;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SchedulerGroup;

import java.net.URI;

/**
 * The Spring-based implementation of {@link Spaces}
 */
@ToString(callSuper = true)
public final class SpringSpaceQuotaDefinitions extends AbstractSpringOperations implements SpaceQuotaDefinitions {

    /**
     * Creates an instance
     *
     * @param restOperations the {@link RestOperations} to use to communicate with the server
     * @param root           the root URI of the server.  Typically something like {@code https://api.run.pivotal.io}.
     * @param schedulerGroup The group to use when making requests
     */
    public SpringSpaceQuotaDefinitions(RestOperations restOperations, URI root, SchedulerGroup schedulerGroup) {
        super(restOperations, root, schedulerGroup);
    }

    @Override
    public Mono<AssociateSpaceQuotaDefinitionResponse> associateSpace(AssociateSpaceQuotaDefinitionRequest request) {
        return put(request, AssociateSpaceQuotaDefinitionResponse.class, builder -> builder.pathSegment("v2", "space_quota_definitions", request.getSpaceQuotaDefinitionId(), "spaces",
            request.getSpaceId()));
    }

    @Override
    public Mono<GetSpaceQuotaDefinitionResponse> get(GetSpaceQuotaDefinitionRequest request) {
        return get(request, GetSpaceQuotaDefinitionResponse.class, builder -> builder.pathSegment("v2", "space_quota_definitions", request.getSpaceQuotaDefinitionId()));
    }

    @Override
    public Mono<ListSpaceQuotaDefinitionsResponse> list(ListSpaceQuotaDefinitionsRequest request) {
        return get(request, ListSpaceQuotaDefinitionsResponse.class, builder -> {
            builder.pathSegment("v2", "space_quota_definitions");
            QueryBuilder.augment(builder, request);
        });
    }

    @Override
    public Mono<Void> removeSpace(RemoveSpaceQuotaDefinitionRequest request) {
        return delete(request, Void.class, builder -> builder.pathSegment("v2", "space_quota_definitions", request.getSpaceQuotaDefinitionId(), "spaces", request.getSpaceId()));
    }

}
