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

package org.cloudfoundry;

import org.cloudfoundry.client.CloudFoundryClient;
import org.cloudfoundry.client.v3.applications.CreateApplicationRequest;
import org.cloudfoundry.client.v3.applications.CreateApplicationResponse;
import org.cloudfoundry.client.v3.packages.CreatePackageRequest;
import org.cloudfoundry.client.v3.packages.CreatePackageResponse;
import org.cloudfoundry.client.v3.packages.StagePackageRequest;
import org.cloudfoundry.client.v3.packages.StagePackageResponse;
import org.cloudfoundry.client.v3.packages.UploadPackageRequest;
import org.cloudfoundry.client.v3.packages.UploadPackageResponse;
import org.cloudfoundry.client.v3.tasks.CreateTaskRequest;
import org.cloudfoundry.client.v3.tasks.CreateTaskResponse;
import org.cloudfoundry.client.v3.tasks.GetTaskRequest;
import org.cloudfoundry.client.v3.tasks.Task;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.cloudfoundry.operations.applications.GetApplicationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static org.cloudfoundry.util.tuple.TupleUtils.function;

final class TasksExample {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private CloudFoundryClient cloudFoundryClient;

    private CloudFoundryOperations operations;

    public void name() {
        requestCreateApplication(cloudFoundryClient)
            .then(createApplicationResponse -> {
                return requestCreatePackage(cloudFoundryClient, createApplicationResponse.getId(), logger)
                    .and(Mono.just(createApplicationResponse.getId()));
            })
            .then(function((createPackageResponse, applicationId) -> {
                return requestUploadPackage(cloudFoundryClient, createPackageResponse.getId())
                    .and(Mono.just(applicationId));
            }))
            .then(function((uploadPackageResponse, applicationId) -> {
                return requestStagePackage(cloudFoundryClient, uploadPackageResponse.getId())
                    .and(Mono.just(applicationId));
            }))
            .then(function((stagePackageResponse, applicationId) -> {
                return requestCreateTask(cloudFoundryClient, applicationId);
            }))
            .subscribe();
    }

    public TaskStatus status(String id) {

        return operations.applications()
            .get(GetApplicationRequest.builder() // convert task id to appId
                .name(id)
                .build())
            .then(applicationDetail ->
                    client.tasks()
                        .get(GetTaskRequest.builder()
                            .id(applicationDetail.getId())
                            .build())
                        .then(getTaskResponse -> {
                            if (getTaskResponse.getState().equals(Task.SUCCEEDED_STATE)) {
                                return Mono.just(new TaskStatus(id, LaunchState.complete, null));
                            } else if (getTaskResponse.getState().equals(Task.RUNNING_STATE)) {
                                return Mono.just(new TaskStatus(id, LaunchState.running, null));
                            } else if (getTaskResponse.getState().equals(Task.FAILED_STATE)) {
                                return Mono.just(new TaskStatus(id, LaunchState.failed, null));
//                            } else if (getTaskResponse.getState().equals(Task.CANCELED_STATE)) {
//                                return Mono.just(new TaskStatus(id, LaunchState.canceled, null));
                            } else {
                                return Mono.just(new TaskStatus(id, LaunchState.unknown, null));
                            }
                        })
            )
            .get();
    }

    private static Mono<CreateApplicationResponse> requestCreateApplication(CloudFoundryClient cloudFoundryClient) {
        CreateApplicationRequest request = CreateApplicationRequest.builder().name("test-name").build();
        return cloudFoundryClient.applicationsV3().create(request);
    }

    private static Mono<CreatePackageResponse> requestCreatePackage(CloudFoundryClient cloudFoundryClient, String applicationId, final Logger logger) {
        CreatePackageRequest request = CreatePackageRequest.builder().applicationId(applicationId).build();
        return cloudFoundryClient.packages().create(request)
            .doOnSubscribe(subscription -> logger.debug("Starting Create Package"));
    }

    private static Mono<CreateTaskResponse> requestCreateTask(CloudFoundryClient cloudFoundryClient, String applicationId) {
        CreateTaskRequest request = CreateTaskRequest.builder().applicationId(applicationId).command("java -jar").build();
        return cloudFoundryClient.tasks().create(request);
    }

    private static Mono<StagePackageResponse> requestStagePackage(CloudFoundryClient cloudFoundryClient, String packageId) {
        StagePackageRequest request = StagePackageRequest.builder().packageId(packageId).build();
        return cloudFoundryClient.packages().stage(request);
    }

    private static Mono<UploadPackageResponse> requestUploadPackage(CloudFoundryClient cloudFoundryClient, String packageId) {
        UploadPackageRequest request = UploadPackageRequest.builder().packageId(packageId).build();
        return cloudFoundryClient.packages().upload(request);
    }

}
