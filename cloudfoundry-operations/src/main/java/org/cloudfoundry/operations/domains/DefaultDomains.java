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

package org.cloudfoundry.operations.domains;

import org.cloudfoundry.client.CloudFoundryClient;
import org.cloudfoundry.client.v2.organizations.ListOrganizationsRequest;
import org.cloudfoundry.client.v2.organizations.OrganizationResource;
import org.cloudfoundry.client.v2.privatedomains.CreatePrivateDomainRequest;
import org.cloudfoundry.client.v2.privatedomains.CreatePrivateDomainResponse;
import org.cloudfoundry.util.ExceptionUtils;
import org.cloudfoundry.util.PaginationUtils;
import org.cloudfoundry.util.ResourceUtils;
import org.cloudfoundry.util.ValidationUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.cloudfoundry.util.tuple.TupleUtils.function;

public final class DefaultDomains implements Domains {

    private final CloudFoundryClient cloudFoundryClient;

    public DefaultDomains(CloudFoundryClient cloudFoundryClient) {
        this.cloudFoundryClient = cloudFoundryClient;
    }

    public Mono<Void> create(CreateDomainRequest request) {
        return ValidationUtils
            .validate(request)
            .then(request1 -> getOrganizationId(this.cloudFoundryClient, request1.getOrganization())
                .and(Mono.just(request1)))
            .then(function((domainId, request1) -> requestCreateDomain(this.cloudFoundryClient, request1.getDomain(), domainId)))
            .after();
    }

    private static Mono<OrganizationResource> getOrganization(CloudFoundryClient cloudFoundryClient, String organization) {
        return requestOrganizations(cloudFoundryClient, organization)
            .single()
            .otherwise(ExceptionUtils.<OrganizationResource>convert("Organization %s does not exist", organization));
    }

    private static Mono<String> getOrganizationId(CloudFoundryClient cloudFoundryClient, String organization) {
        return getOrganization(cloudFoundryClient, organization)
            .map(ResourceUtils::getId);
    }

    private static Mono<CreatePrivateDomainResponse> requestCreateDomain(CloudFoundryClient cloudFoundryClient, String domain, String organizationId) {
        return cloudFoundryClient.privateDomains()
            .create(CreatePrivateDomainRequest.builder()
                .name(domain)
                .owningOrganizationId(organizationId)
                .build());
    }

    private static Flux<OrganizationResource> requestOrganizations(CloudFoundryClient cloudFoundryClient, String organization) {
        return PaginationUtils
            .requestResources(page -> cloudFoundryClient.organizations().list(
                ListOrganizationsRequest.builder()
                    .name(organization)
                    .page(page)
                    .build()));
    }

}
