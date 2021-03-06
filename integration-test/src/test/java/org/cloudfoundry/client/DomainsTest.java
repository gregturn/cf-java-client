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

package org.cloudfoundry.client;

import org.cloudfoundry.AbstractIntegrationTest;
import org.cloudfoundry.client.v2.applications.CreateApplicationRequest;
import org.cloudfoundry.client.v2.domains.CreateDomainRequest;
import org.cloudfoundry.client.v2.domains.CreateDomainResponse;
import org.cloudfoundry.client.v2.domains.DeleteDomainRequest;
import org.cloudfoundry.client.v2.domains.DomainEntity;
import org.cloudfoundry.client.v2.domains.GetDomainRequest;
import org.cloudfoundry.client.v2.domains.ListDomainSpacesRequest;
import org.cloudfoundry.client.v2.domains.ListDomainsRequest;
import org.cloudfoundry.client.v2.routes.AssociateRouteApplicationRequest;
import org.cloudfoundry.client.v2.routes.CreateRouteRequest;
import org.cloudfoundry.client.v2.spaces.GetSpaceRequest;
import org.cloudfoundry.client.v2.spaces.SpaceEntity;
import org.cloudfoundry.util.JobUtils;
import org.cloudfoundry.util.PaginationUtils;
import org.cloudfoundry.util.ResourceUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Mono;
import reactor.core.tuple.Tuple2;

import java.util.function.Consumer;

import static org.cloudfoundry.util.OperationUtils.thenKeep;
import static org.cloudfoundry.util.tuple.TupleUtils.consumer;
import static org.cloudfoundry.util.tuple.TupleUtils.function;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DomainsTest extends AbstractIntegrationTest {

    @Autowired
    private CloudFoundryClient cloudFoundryClient;

    @Autowired
    private Mono<String> organizationId;

    @Autowired
    private Mono<String> spaceId;

    @Test
    public void create() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    createDomainEntity(this.cloudFoundryClient, organizationId, domainName),
                    Mono.just(organizationId)
                ))
            .subscribe(this.<Tuple2<DomainEntity, String>>testSubscriber()
                .assertThat(entityMatchesDomainNameAndOrganizationId(domainName)));
    }

    @Test
    public void delete() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> createDomainId(this.cloudFoundryClient, organizationId, domainName))
            .then(domainId -> this.cloudFoundryClient.domains()
                .delete(DeleteDomainRequest.builder()
                    .async(true)
                    .domainId(domainId)
                    .build())
                .map(ResourceUtils::getId)
                .then(jobId -> JobUtils.waitForCompletion(this.cloudFoundryClient, jobId)))
            .subscribe(testSubscriber());
    }

    @Test
    public void get() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    Mono.just(organizationId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                ))
            .then(function((organizationId, domainId) -> Mono
                .when(this.cloudFoundryClient.domains()
                        .get(GetDomainRequest.builder()
                            .domainId(domainId)
                            .build())
                        .map(ResourceUtils::getEntity),
                    Mono.just(organizationId)
                )))
            .subscribe(this.<Tuple2<DomainEntity, String>>testSubscriber()
                .assertThat(entityMatchesDomainNameAndOrganizationId(domainName)));
    }

    @Test
    public void list() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    Mono.just(organizationId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                ))
            .then(function((organizationId, domainId) -> Mono
                .when(
                    PaginationUtils
                        .requestResources(page -> this.cloudFoundryClient.domains()
                            .list(ListDomainsRequest.builder()
                                .page(page)
                                .build()))
                        .filter(resource -> domainId.equals(ResourceUtils.getId(resource)))
                        .single()
                        .map(ResourceUtils::getEntity),
                    Mono.just(organizationId)
                )))
            .subscribe(this.<Tuple2<DomainEntity, String>>testSubscriber()
                .assertThat(entityMatchesDomainNameAndOrganizationId(domainName)));
    }

    @Test
    public void listDomainSpaces() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> createDomainId(this.cloudFoundryClient, organizationId, domainName))
            .flatMap(domainId -> PaginationUtils
                .requestResources(page -> this.cloudFoundryClient.domains()
                    .listSpaces(ListDomainSpacesRequest.builder()
                        .domainId(domainId)
                        .page(page)
                        .build())))
            .count()
            .subscribe(this.<Long>testSubscriber()
                .assertThat(count -> assertTrue(count > 0)));
    }

    @Test
    public void listDomainSpacesFilterByApplicationId() {
        String applicationName = getApplicationName();
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> createDomainId(this.cloudFoundryClient, organizationId, domainName))
            .and(this.spaceId)
            .then(function((domainId, spaceId) -> Mono
                .when(
                    Mono.just(domainId),
                    Mono.just(spaceId),
                    this.cloudFoundryClient.applicationsV2()
                        .create(CreateApplicationRequest.builder()
                            .name(applicationName)
                            .spaceId(spaceId)
                            .build())
                        .map(ResourceUtils::getId),
                    this.cloudFoundryClient.routes()
                        .create(CreateRouteRequest.builder()
                            .domainId(domainId)
                            .spaceId(spaceId)
                            .build())
                        .map(ResourceUtils::getId)
                )))
            .as(thenKeep(function((domainId, spaceId, applicationId, routeId) -> this.cloudFoundryClient.routes()
                .associateApplication(AssociateRouteApplicationRequest.builder()
                    .routeId(routeId)
                    .applicationId(applicationId)
                    .build())
            )))
            .then(function((domainId, spaceId, applicationId, routeId) -> Mono
                .when(
                    PaginationUtils
                        .requestResources(page ->
                            this.cloudFoundryClient.domains()
                                .listSpaces(ListDomainSpacesRequest.builder()
                                    .page(page)
                                    .applicationId(applicationId)
                                    .domainId(domainId)
                                    .build())
                        )
                        .single()
                        .map(ResourceUtils::getId),
                    Mono.just(spaceId)
                )))
            .subscribe(this.<Tuple2<String, String>>testSubscriber()
                .assertThat(this::assertTupleEquality));
    }

    @Ignore("TODO: implement once list users available https://www.pivotaltracker.com/story/show/101522708")
    @Test
    public void listDomainSpacesFilterByDeveloperId() {
        Assert.fail();
    }

    @Test
    public void listDomainSpacesFilterByName() {
        String domainName = getDomainName();

        Mono
            .when(this.organizationId, this.spaceId)
            .then(function((organizationId, spaceId) -> Mono
                .when(
                    Mono.just(spaceId),
                    getSpaceName(this.cloudFoundryClient, spaceId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                )))
            .then(function((spaceId, spaceName, domainId) -> Mono
                .when(
                    PaginationUtils
                        .requestResources(page -> this.cloudFoundryClient.domains()
                            .listSpaces(ListDomainSpacesRequest.builder()
                                .domainId(domainId)
                                .name(spaceName)
                                .page(page)
                                .build()))
                        .single()
                        .map(ResourceUtils::getId),
                    Mono.just(spaceId)
                )))
            .subscribe(this.<Tuple2<String, String>>testSubscriber()
                .assertThat(this::assertTupleEquality));
    }

    @Test
    public void listDomainSpacesFilterByOrganizationId() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    Mono.just(organizationId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                ))
            .flatMap(function((organizationId, domainId) -> PaginationUtils
                .requestResources(page -> this.cloudFoundryClient.domains()
                    .listSpaces(ListDomainSpacesRequest.builder()
                        .domainId(domainId)
                        .organizationId(organizationId)
                        .page(page)
                        .build()))))
            .count()
            .subscribe(this.<Long>testSubscriber()
                .assertThat(count -> assertTrue(count > 0)));
    }

    @Test
    public void listFilterByName() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    Mono.just(organizationId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                ))
            .then(function((organizationId, domainId) -> Mono
                .when(
                    PaginationUtils
                        .requestResources(page -> this.cloudFoundryClient.domains()
                            .list(ListDomainsRequest.builder()
                                .name(domainName)
                                .page(page)
                                .build()))
                        .filter(resource -> domainId.equals(ResourceUtils.getId(resource)))
                        .single()
                        .map(ResourceUtils::getEntity),
                    Mono.just(organizationId)
                )))
            .subscribe(this.<Tuple2<DomainEntity, String>>testSubscriber()
                .assertThat(entityMatchesDomainNameAndOrganizationId(domainName)));
    }

    @Test
    public void listFilterByOwningOrganizationId() {
        String domainName = getDomainName();

        this.organizationId
            .then(organizationId -> Mono
                .when(
                    Mono.just(organizationId),
                    createDomainId(this.cloudFoundryClient, organizationId, domainName)
                ))
            .then(function((organizationId, domainId) -> Mono
                .when(
                    PaginationUtils
                        .requestResources(page -> this.cloudFoundryClient.domains()
                            .list(ListDomainsRequest.builder()
                                .owningOrganizationId(organizationId)
                                .page(page)
                                .build()))
                        .filter(resource -> domainId.equals(ResourceUtils.getId(resource)))
                        .single()
                        .map(ResourceUtils::getEntity),
                    Mono.just(organizationId)
                )))
            .subscribe(this.<Tuple2<DomainEntity, String>>testSubscriber()
                .assertThat(entityMatchesDomainNameAndOrganizationId(domainName)));
    }

    private static Mono<CreateDomainResponse> createDomain(CloudFoundryClient cloudFoundryClient, String organizationId, String domainName) {
        return cloudFoundryClient.domains()
            .create(CreateDomainRequest.builder()
                .name(domainName)
                .owningOrganizationId(organizationId)
                .wildcard(true)
                .build());
    }

    private static Mono<DomainEntity> createDomainEntity(CloudFoundryClient cloudFoundryClient, String organizationId, String domainName) {
        return createDomain(cloudFoundryClient, organizationId, domainName)
            .map(ResourceUtils::getEntity);
    }

    private static Mono<String> createDomainId(CloudFoundryClient cloudFoundryClient, String organizationId, String domainName) {
        return createDomain(cloudFoundryClient, organizationId, domainName)
            .map(ResourceUtils::getId);
    }

    private static Consumer<Tuple2<DomainEntity, String>> entityMatchesDomainNameAndOrganizationId(String domainName) {
        return consumer((entity, organizationId) -> {
            assertEquals(domainName, entity.getName());
            assertEquals(organizationId, entity.getOwningOrganizationId());
        });
    }

    private static Mono<String> getSpaceName(CloudFoundryClient cloudFoundryClient, String spaceId) {
        return cloudFoundryClient.spaces()
            .get(GetSpaceRequest.builder()
                .spaceId(spaceId)
                .build())
            .map(ResourceUtils::getEntity)
            .map(SpaceEntity::getName);
    }

}
