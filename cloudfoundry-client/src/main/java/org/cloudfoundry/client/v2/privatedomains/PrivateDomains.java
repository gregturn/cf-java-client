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

package org.cloudfoundry.client.v2.privatedomains;

import reactor.core.publisher.Mono;

/**
 * Main entry point to the Cloud Foundry Domains Client API
 */
public interface PrivateDomains {

    /**
     * Makes the <a href="http://apidocs.cloudfoundry.org/latest-release/private_domains/create_a_private_domain_owned_by_the_given_organization.html">Create a Private Domain owned by the given
     * Organization</a> request
     *
     * @param request the Create a Private Domain request
     * @return the response from the Create a Private Domain request
     */
    Mono<CreatePrivateDomainResponse> create(CreatePrivateDomainRequest request);

    /**
     * Makes the <a href="http://apidocs.cloudfoundry.org/latest-release/private_domains/delete_a_particular_private_domain.html">Delete a Particular Private Domain</a> request
     *
     * @param request the Delete Private Domain request
     * @return the response from the Delete Private Domain request
     */
    Mono<DeletePrivateDomainResponse> delete(DeletePrivateDomainRequest request);

    /**
     * Makes the <a href="http://apidocs.cloudfoundry.org/latest-release/private_domains/filtering_private_domains_by_name.html">List Private Domains</a> request
     *
     * @param request the List Private Domains request
     * @return the response from the List Private Domains request
     */
    Mono<ListPrivateDomainsResponse> list(ListPrivateDomainsRequest request);
}
