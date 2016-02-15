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

package org.cloudfoundry.operations.applications;

import org.cloudfoundry.client.CloudFoundryClient;
import org.cloudfoundry.client.v2.PaginatedRequest;
import org.cloudfoundry.client.v2.applications.AbstractApplicationResource;
import org.cloudfoundry.client.v2.applications.ApplicationEnvironmentRequest;
import org.cloudfoundry.client.v2.applications.ApplicationEnvironmentResponse;
import org.cloudfoundry.client.v2.applications.ApplicationInstanceInfo;
import org.cloudfoundry.client.v2.applications.ApplicationInstancesRequest;
import org.cloudfoundry.client.v2.applications.ApplicationInstancesResponse;
import org.cloudfoundry.client.v2.applications.ApplicationResource;
import org.cloudfoundry.client.v2.applications.ApplicationStatisticsRequest;
import org.cloudfoundry.client.v2.applications.ApplicationStatisticsResponse;
import org.cloudfoundry.client.v2.applications.GetApplicationResponse;
import org.cloudfoundry.client.v2.applications.RestageApplicationResponse;
import org.cloudfoundry.client.v2.applications.SummaryApplicationRequest;
import org.cloudfoundry.client.v2.applications.SummaryApplicationResponse;
import org.cloudfoundry.client.v2.applications.TerminateApplicationInstanceRequest;
import org.cloudfoundry.client.v2.applications.UpdateApplicationRequest;
import org.cloudfoundry.client.v2.applications.UpdateApplicationResponse;
import org.cloudfoundry.client.v2.events.EventEntity;
import org.cloudfoundry.client.v2.events.EventResource;
import org.cloudfoundry.client.v2.events.ListEventsRequest;
import org.cloudfoundry.client.v2.events.ListEventsResponse;
import org.cloudfoundry.client.v2.routes.DeleteRouteRequest;
import org.cloudfoundry.client.v2.routes.DeleteRouteResponse;
import org.cloudfoundry.client.v2.routes.Route;
import org.cloudfoundry.client.v2.serviceinstances.ServiceInstance;
import org.cloudfoundry.client.v2.spaces.GetSpaceSummaryRequest;
import org.cloudfoundry.client.v2.spaces.GetSpaceSummaryResponse;
import org.cloudfoundry.client.v2.spaces.ListSpaceApplicationsRequest;
import org.cloudfoundry.client.v2.spaces.ListSpaceApplicationsResponse;
import org.cloudfoundry.client.v2.spaces.SpaceApplicationSummary;
import org.cloudfoundry.client.v2.stacks.GetStackRequest;
import org.cloudfoundry.client.v2.stacks.GetStackResponse;
import org.cloudfoundry.logging.LogMessage;
import org.cloudfoundry.logging.LoggingClient;
import org.cloudfoundry.logging.RecentLogsRequest;
import org.cloudfoundry.logging.StreamLogsRequest;
import org.cloudfoundry.util.DateUtils;
import org.cloudfoundry.util.DelayUtils;
import org.cloudfoundry.util.ExceptionUtils;
import org.cloudfoundry.util.JobUtils;
import org.cloudfoundry.util.OperationUtils;
import org.cloudfoundry.util.Optional;
import org.cloudfoundry.util.OptionalUtils;
import org.cloudfoundry.util.PaginationUtils;
import org.cloudfoundry.util.ResourceUtils;
import org.cloudfoundry.util.StringMap;
import org.cloudfoundry.util.ValidationUtils;
import org.cloudfoundry.util.tuple.Function2;
import org.cloudfoundry.util.tuple.Function3;
import org.cloudfoundry.util.tuple.Function4;
import org.cloudfoundry.util.tuple.Predicate2;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.fn.BiFunction;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.rx.Stream;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.cloudfoundry.util.DateUtils.parseFromIso8601;
import static org.cloudfoundry.util.OperationUtils.afterComplete;
import static org.cloudfoundry.util.OperationUtils.not;
import static org.cloudfoundry.util.tuple.TupleUtils.function;
import static org.cloudfoundry.util.tuple.TupleUtils.predicate;

public final class DefaultApplications implements Applications {

    private static final int MAX_NUMBER_OF_RECENT_EVENTS = 50;

    private static final String STARTED_STATE = "STARTED";

    private static final String STOPPED_STATE = "STOPPED";

    private final CloudFoundryClient cloudFoundryClient;

    private final LoggingClient loggingClient;

    private final Mono<String> spaceId;

    public DefaultApplications(CloudFoundryClient cloudFoundryClient, LoggingClient loggingClient, Mono<String> spaceId) {
        this.cloudFoundryClient = cloudFoundryClient;
        this.loggingClient = loggingClient;
        this.spaceId = spaceId;
    }

    @Override
    public Mono<Void> delete(DeleteApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<DeleteApplicationRequest, String, Mono<Tuple2<Optional<List<Route>>, String>>>() {

                @Override
                public Mono<Tuple2<Optional<List<Route>>, String>> apply(DeleteApplicationRequest request, String spaceId) {
                    return getRoutesAndApplicationId(DefaultApplications.this.cloudFoundryClient, request, spaceId);
                }

            }))
            .then(function(new Function2<Optional<List<Route>>, String, Mono<String>>() {

                @Override
                public Mono<String> apply(Optional<List<Route>> routes, final String applicationId) {
                    return deleteRoutes(DefaultApplications.this.cloudFoundryClient, routes)
                        .as(afterComplete(new Supplier<Mono<String>>() {

                            @Override
                            public Mono<String> get() {
                                return Mono.just(applicationId);
                            }

                        }));
                }

            }))
            .then(new Function<String, Mono<Void>>() {

                @Override
                public Mono<Void> apply(String applicationId) {
                    return requestDeleteApplication(DefaultApplications.this.cloudFoundryClient, applicationId);
                }

            });
    }

    @Override
    public Mono<Void> disableSsh(DisableApplicationSshRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<DisableApplicationSshRequest, String, Mono<String>>() {

                @Override
                public Mono<String> apply(DisableApplicationSshRequest request, String spaceId) {
                    return getApplicationIdWhere(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId, sshEnabled(true));
                }

            }))
            .then(new Function<String, Mono<AbstractApplicationResource>>() {

                @Override
                public Mono<AbstractApplicationResource> apply(String applicationId) {
                    return requestUpdateApplicationSsh(DefaultApplications.this.cloudFoundryClient, applicationId, false);
                }

            })
            .after();
    }

    @Override
    public Mono<Void> enableSsh(EnableApplicationSshRequest request) {
        return Mono
            .when(ValidationUtils.validate(request), this.spaceId)
            .then(function(new Function2<EnableApplicationSshRequest, String, Mono<String>>() {

                @Override
                public Mono<String> apply(EnableApplicationSshRequest request, String spaceId) {
                    return getApplicationIdWhere(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId, sshEnabled(false));
                }

            }))
            .then(new Function<String, Mono<AbstractApplicationResource>>() {

                @Override
                public Mono<AbstractApplicationResource> apply(String applicationId) {
                    return requestUpdateApplicationSsh(DefaultApplications.this.cloudFoundryClient, applicationId, true);
                }

            })
            .after();
    }

    @Override
    public Mono<ApplicationDetail> get(GetApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils
                    .validate(request)
                    .map(new Function<GetApplicationRequest, String>() {

                        @Override
                        public String apply(GetApplicationRequest request) {
                            return request.getName();
                        }

                    }),
                this.spaceId
            )
            .then(function(new Function2<String, String, Mono<AbstractApplicationResource>>() {

                @Override
                public Mono<AbstractApplicationResource> apply(String application, String spaceId) {
                    return getApplication(DefaultApplications.this.cloudFoundryClient, application, spaceId);
                }

            }))
            .then(new Function<AbstractApplicationResource, Mono<Tuple4<ApplicationStatisticsResponse, SummaryApplicationResponse, GetStackResponse, ApplicationInstancesResponse>>>() {

                @Override
                public Mono<Tuple4<ApplicationStatisticsResponse, SummaryApplicationResponse, GetStackResponse, ApplicationInstancesResponse>> apply(AbstractApplicationResource applicationResource) {
                    return getAuxiliaryContent(DefaultApplications.this.cloudFoundryClient, applicationResource);
                }

            })
            .map(function(new Function4<ApplicationStatisticsResponse, SummaryApplicationResponse, GetStackResponse, ApplicationInstancesResponse, ApplicationDetail>() {

                @Override
                public ApplicationDetail apply(ApplicationStatisticsResponse applicationStatisticsResponse, SummaryApplicationResponse summaryApplicationResponse, GetStackResponse getStackResponse,
                                               ApplicationInstancesResponse applicationInstancesResponse) {
                    return toApplicationDetail(applicationStatisticsResponse, summaryApplicationResponse, getStackResponse, applicationInstancesResponse);
                }

            }));
    }

    @Override
    public Mono<ApplicationManifest> getApplicationManifest(GetApplicationManifestRequest request) {
        return ValidationUtils
            .validate(request)
            .and(this.spaceId)
            .then(function(new Function2<GetApplicationManifestRequest, String, Mono<String>>() {

                @Override
                public Mono<String> apply(GetApplicationManifestRequest request, String spaceId) {
                    return getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId);
                }

            }))
            .then(new Function<String, Mono<SummaryApplicationResponse>>() {

                @Override
                public Mono<SummaryApplicationResponse> apply(String applicationId) {
                    return requestApplicationSummary(DefaultApplications.this.cloudFoundryClient, applicationId);
                }

            })
            .then(new Function<SummaryApplicationResponse, Mono<Tuple2<SummaryApplicationResponse, String>>>() {

                @Override
                public Mono<Tuple2<SummaryApplicationResponse, String>> apply(SummaryApplicationResponse summaryApplicationResponse) {
                    return Mono
                        .just(summaryApplicationResponse)
                        .and(requestStackName(cloudFoundryClient, summaryApplicationResponse.getStackId()));
                }

            })
            .then(function(new Function2<SummaryApplicationResponse, String, Mono<ApplicationManifest>>() {

                @Override
                public Mono<ApplicationManifest> apply(SummaryApplicationResponse summaryApplicationResponse, String stackName) {
                    return toApplicationManifest(summaryApplicationResponse, stackName);
                }

            }));
    }

    @Override
    public Mono<ApplicationEnvironments> getEnvironments(GetApplicationEnvironmentsRequest request) {
        return ValidationUtils
            .validate(request)
            .and(this.spaceId)
            .then(function(new Function2<GetApplicationEnvironmentsRequest, String, Mono<String>>() {

                @Override
                public Mono<String> apply(GetApplicationEnvironmentsRequest request, String spaceId) {
                    return getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId);
                }

            }))
            .then(new Function<String, Mono<ApplicationEnvironmentResponse>>() {

                @Override
                public Mono<ApplicationEnvironmentResponse> apply(String applicationId) {
                    return requestApplicationEnvironment(DefaultApplications.this.cloudFoundryClient, applicationId);
                }

            })
            .map(new Function<ApplicationEnvironmentResponse, ApplicationEnvironments>() {

                @Override
                public ApplicationEnvironments apply(ApplicationEnvironmentResponse response) {
                    return toApplicationEnvironments(response);
                }

            });
    }

    @Override
    public Publisher<ApplicationEvent> getEvents(GetApplicationEventsRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<GetApplicationEventsRequest, String, Mono<Tuple2<GetApplicationEventsRequest, String>>>() {

                @Override
                public Mono<Tuple2<GetApplicationEventsRequest, String>> apply(GetApplicationEventsRequest request, String spaceId) {
                    return Mono.when(Mono.just(request), getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId));
                }

            }))
            .flatMap(function(new Function2<GetApplicationEventsRequest, String, Stream<EventResource>>() {

                @Override
                public Stream<EventResource> apply(GetApplicationEventsRequest request, final String applicationId) {
                    return getEventResources(applicationId, DefaultApplications.this.cloudFoundryClient)
                        .take(Optional.ofNullable(request.getMaxNumberOfEvents()).orElse(MAX_NUMBER_OF_RECENT_EVENTS));

                }

            }))
            .map(new Function<EventResource, ApplicationEvent>() {

                @Override
                public ApplicationEvent apply(EventResource resource) {
                    return convertToApplicationEvent(resource);
                }

            });
    }

    @Override
    public Publisher<ApplicationSummary> list() {
        return this.spaceId
            .then(new Function<String, Mono<GetSpaceSummaryResponse>>() {

                @Override
                public Mono<GetSpaceSummaryResponse> apply(String spaceId) {
                    return requestSpaceSummary(DefaultApplications.this.cloudFoundryClient, spaceId);
                }

            })
            .flatMap(new Function<GetSpaceSummaryResponse, Stream<SpaceApplicationSummary>>() {

                @Override
                public Stream<SpaceApplicationSummary> apply(GetSpaceSummaryResponse response) {
                    return extractApplications(response);
                }

            })
            .map(new Function<SpaceApplicationSummary, ApplicationSummary>() {

                @Override
                public ApplicationSummary apply(SpaceApplicationSummary spaceApplicationSummary) {
                    return toApplicationSummary(spaceApplicationSummary);
                }

            });
    }

    @Override
    public Publisher<LogMessage> logs(LogsRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<LogsRequest, String, Mono<Tuple2<String, LogsRequest>>>() {

                @Override
                public Mono<Tuple2<String, LogsRequest>> apply(LogsRequest request, String spaceId) {
                    return getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId)
                        .and(Mono.just(request));
                }

            }))
            .flatMap(function(new Function2<String, LogsRequest, Publisher<LogMessage>>() {

                @Override
                public Publisher<LogMessage> apply(final String applicationId, LogsRequest logsRequest) {
                    return getLogs(DefaultApplications.this.loggingClient, applicationId, logsRequest.getRecent());
                }

            }));
    }

    @Override
    public Mono<Void> rename(RenameApplicationRequest request) {
        return ValidationUtils
            .validate(request)
            .then(new Function<RenameApplicationRequest, Mono<Tuple3<String, String, String>>>() {

                @Override
                public Mono<Tuple3<String, String, String>> apply(RenameApplicationRequest request) {
                    return Mono
                        .when(
                            Mono.just(request.getName()),
                            Mono.just(request.getNewName()),
                            DefaultApplications.this.spaceId
                        );
                }

            })
            .then(function(new Function3<String, String, String, Mono<Tuple2<String, String>>>() {

                @Override
                public Mono<Tuple2<String, String>> apply(String application, String newName, String spaceId) {
                    return Mono
                        .when(
                            getApplicationId(DefaultApplications.this.cloudFoundryClient, application, spaceId),
                            Mono.just(newName)
                        );
                }

            }))
            .then(function(new Function2<String, String, Mono<UpdateApplicationResponse>>() {

                @Override
                public Mono<UpdateApplicationResponse> apply(String applicationId, String newName) {
                    return requestUpdateApplicationRename(DefaultApplications.this.cloudFoundryClient, applicationId, newName);
                }

            }))
            .after();
    }

    @Override
    public Mono<Void> restage(RestageApplicationRequest request) {
        return ValidationUtils
            .validate(request)
            .and(this.spaceId)
            .then(function(new Function2<RestageApplicationRequest, String, Mono<Tuple2<String, RestageApplicationRequest>>>() {

                @Override
                public Mono<Tuple2<String, RestageApplicationRequest>> apply(RestageApplicationRequest request, String spaceId) {
                    return getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId)
                        .and(Mono.just(request));
                }

            }))
            .then(function(new Function2<String, RestageApplicationRequest, Mono<String>>() {

                @Override
                public Mono<String> apply(String applicationId, RestageApplicationRequest request) {
                    return restageApplication(DefaultApplications.this.cloudFoundryClient, request.getName(), applicationId);
                }

            }))
            .after();
    }

    @Override
    public Mono<Void> restart(RestartApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils
                    .validate(request)
                    .map(new Function<RestartApplicationRequest, String>() {

                        @Override
                        public String apply(RestartApplicationRequest request) {
                            return request.getName();
                        }

                    }),
                this.spaceId
            )
            .then(function(new Function2<String, String, Mono<Tuple2<AbstractApplicationResource, String>>>() {

                @Override
                public Mono<Tuple2<AbstractApplicationResource, String>> apply(String application, String spaceId) {
                    return getApplication(DefaultApplications.this.cloudFoundryClient, application, spaceId)
                        .and(Mono.just(application));
                }

            }))
            .then(function(new Function2<AbstractApplicationResource, String, Mono<String>>() {

                @Override
                public Mono<String> apply(final AbstractApplicationResource resource, final String application) {
                    return Mono.just(resource)
                        .as(ifThen(not(isIn(STOPPED_STATE)), new Function<AbstractApplicationResource, Mono<AbstractApplicationResource>>() {

                            @Override
                            public Mono<AbstractApplicationResource> apply(AbstractApplicationResource resource) {
                                return stopApplication(DefaultApplications.this.cloudFoundryClient, ResourceUtils.getId(resource));
                            }

                        }))
                        .then(new Function<AbstractApplicationResource, Mono<String>>() {

                            @Override
                            public Mono<String> apply(AbstractApplicationResource resource) {
                                return startApplicationAndWait(DefaultApplications.this.cloudFoundryClient, application, ResourceUtils.getId(resource));
                            }

                        });
                }

            }))
            .after();
    }

    @Override
    public Mono<Void> restartInstance(RestartApplicationInstanceRequest request) {
        return ValidationUtils
            .validate(request)
            .and(this.spaceId)
            .then(function(new Function2<RestartApplicationInstanceRequest, String, Mono<Tuple2<String, String>>>() {

                @Override
                public Mono<Tuple2<String, String>> apply(RestartApplicationInstanceRequest request, String spaceId) {
                    return Mono
                        .when(
                            getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId),
                            Mono.just(String.valueOf(request.getInstanceIndex()))
                        );
                }

            }))
            .then(function(new Function2<String, String, Mono<Void>>() {

                @Override
                public Mono<Void> apply(String applicationId, String instanceIndex) {
                    return requestTerminateApplicationInstance(DefaultApplications.this.cloudFoundryClient, applicationId, instanceIndex);
                }

            }));
    }

    @Override
    public Mono<Void> scale(final ScaleApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils
                    .validate(request)
                    .where(new Predicate<ScaleApplicationRequest>() {

                        @Override
                        public boolean test(ScaleApplicationRequest request) {
                            return areModifiersPresent(request);
                        }

                    }),
                this.spaceId
            )
            .then(function(new Function2<ScaleApplicationRequest, String, Mono<Tuple2<ScaleApplicationRequest, String>>>() {

                @Override
                public Mono<Tuple2<ScaleApplicationRequest, String>> apply(ScaleApplicationRequest request, String spaceId) {
                    return Mono
                        .when(
                            Mono.just(request),
                            getApplicationId(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId)
                        );
                }
            }))
            .then(function(new Function2<ScaleApplicationRequest, String, Mono<Tuple2<ScaleApplicationRequest, AbstractApplicationResource>>>() {

                @Override
                public Mono<Tuple2<ScaleApplicationRequest, AbstractApplicationResource>> apply(ScaleApplicationRequest request, String applicationId) {
                    return Mono
                        .when(
                            Mono.just(request),
                            requestUpdateApplicationScale(cloudFoundryClient, applicationId, request.getDiskLimit(), request.getInstances(), request.getMemoryLimit())
                        );
                }
            }))
            .where(predicate(new Predicate2<ScaleApplicationRequest, AbstractApplicationResource>() {

                @Override
                public boolean test(ScaleApplicationRequest request, AbstractApplicationResource resource) {
                    return isRestartRequired(request, resource);
                }
            }))
            .then(function(new Function2<ScaleApplicationRequest, AbstractApplicationResource, Mono<String>>() {

                @Override
                public Mono<String> apply(ScaleApplicationRequest request, AbstractApplicationResource resource) {
                    return restartApplication(DefaultApplications.this.cloudFoundryClient, request.getName(), resource);
                }

            }))
            .after();
    }

    @Override
    public Mono<Void> setEnvironmentVariable(SetEnvironmentVariableApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<SetEnvironmentVariableApplicationRequest, String, Mono<Tuple2<SetEnvironmentVariableApplicationRequest, AbstractApplicationResource>>>() {

                @Override
                public Mono<Tuple2<SetEnvironmentVariableApplicationRequest, AbstractApplicationResource>> apply(SetEnvironmentVariableApplicationRequest request, String spaceId) {
                    return Mono
                        .when(
                            Mono.just(request),
                            getApplication(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId)
                        );
                }

            }))
            .then(function(new Function2<SetEnvironmentVariableApplicationRequest, AbstractApplicationResource, Mono<UpdateApplicationResponse>>() {

                @Override
                public Mono<UpdateApplicationResponse> apply(SetEnvironmentVariableApplicationRequest request, AbstractApplicationResource resource) {
                    return requestUpdateApplicationEnvironment(DefaultApplications.this.cloudFoundryClient, ResourceUtils.getId(resource),
                        addToEnvironment(getEnvironment(resource), request.getVariableName(), request.getVariableValue()));
                }

            }))
            .after();
    }

    @Override
    public Mono<Boolean> sshEnabled(ApplicationSshEnabledRequest request) {
        return Mono
            .when(ValidationUtils.validate(request), this.spaceId)
            .then(function(new Function2<ApplicationSshEnabledRequest, String, Mono<AbstractApplicationResource>>() {

                @Override
                public Mono<AbstractApplicationResource> apply(ApplicationSshEnabledRequest request, String spaceId) {
                    return getApplication(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId);
                }

            }))
            .map(new Function<AbstractApplicationResource, Boolean>() {

                @Override
                public Boolean apply(AbstractApplicationResource applicationResource) {
                    return ResourceUtils.getEntity(applicationResource).getEnableSsh();
                }

            });
    }

    @Override
    public Mono<Void> start(StartApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils
                    .validate(request)
                    .map(new Function<StartApplicationRequest, String>() {

                        @Override
                        public String apply(StartApplicationRequest request) {
                            return request.getName();
                        }

                    }),
                this.spaceId
            )
            .then(function(new Function2<String, String, Mono<Tuple2<String, String>>>() {

                @Override
                public Mono<Tuple2<String, String>> apply(String application, String spaceId) {
                    return getApplicationIdWhere(DefaultApplications.this.cloudFoundryClient, application, spaceId, not(isIn(STARTED_STATE)))
                        .and(Mono.just(application));
                }

            }))
            .then(function(new Function2<String, String, Mono<String>>() {

                @Override
                public Mono<String> apply(String applicationId, String application) {
                    return startApplicationAndWait(DefaultApplications.this.cloudFoundryClient, application, applicationId);
                }

            }))
            .after();
    }

    @Override
    public Mono<Void> stop(StopApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils
                    .validate(request)
                    .map(new Function<StopApplicationRequest, String>() {

                        @Override
                        public String apply(StopApplicationRequest request) {
                            return request.getName();
                        }

                    }),
                this.spaceId
            )
            .then(function(new Function2<String, String, Mono<String>>() {

                @Override
                public Mono<String> apply(String application, String spaceId) {
                    return getApplicationIdWhere(DefaultApplications.this.cloudFoundryClient, application, spaceId, not(isIn(STOPPED_STATE)));
                }

            }))
            .then(new Function<String, Mono<AbstractApplicationResource>>() {

                @Override
                public Mono<AbstractApplicationResource> apply(String applicationId) {
                    return stopApplication(DefaultApplications.this.cloudFoundryClient, applicationId);
                }

            })
            .after();
    }

    @Override
    public Mono<Void> unsetEnvironmentVariable(UnsetEnvironmentVariableApplicationRequest request) {
        return Mono
            .when(
                ValidationUtils.validate(request),
                this.spaceId
            )
            .then(function(new Function2<UnsetEnvironmentVariableApplicationRequest, String, Mono<Tuple2<UnsetEnvironmentVariableApplicationRequest, AbstractApplicationResource>>>() {

                @Override
                public Mono<Tuple2<UnsetEnvironmentVariableApplicationRequest, AbstractApplicationResource>> apply(UnsetEnvironmentVariableApplicationRequest request, String spaceId) {
                    return Mono.when(Mono.just(request), getApplication(DefaultApplications.this.cloudFoundryClient, request.getName(), spaceId));
                }

            }))
            .then(function(new Function2<UnsetEnvironmentVariableApplicationRequest, AbstractApplicationResource, Mono<UpdateApplicationResponse>>() {

                @Override
                public Mono<UpdateApplicationResponse> apply(UnsetEnvironmentVariableApplicationRequest request, AbstractApplicationResource resource) {
                    return requestUpdateApplicationEnvironment(DefaultApplications.this.cloudFoundryClient, ResourceUtils.getId(resource),
                        removeFromEnvironment(getEnvironment(resource), request.getVariableName()));
                }

            }))
            .after();
    }

    private static Map<String, Object> addToEnvironment(Map<String, Object> environment, String variableName, Object variableValue) {
        return StringMap.builder()
            .entries(environment)
            .entry(variableName, variableValue)
            .build();
    }

    private static boolean areModifiersPresent(ScaleApplicationRequest request) {
        return request.getMemoryLimit() != null || request.getDiskLimit() != null || request.getInstances() != null;
    }

    private static BiFunction<String, String, String> collectStates() {
        return new BiFunction<String, String, String>() {

            @Override
            public String apply(String totalState, String instanceState) {
                if ("RUNNING".equals(instanceState) || "RUNNING".equals(totalState)) {
                    return "RUNNING";
                }

                if ("FLAPPING".equals(instanceState) || "CRASHED".equals(instanceState)) {
                    return "FAILED";
                }

                return totalState;
            }

        };
    }

    private static ApplicationEvent convertToApplicationEvent(EventResource resource) {
        EventEntity entity = resource.getEntity();
        Date timestamp = null;
        try {
            timestamp = parseFromIso8601(entity.getTimestamp());
        } catch (IllegalArgumentException iae) {
            // do not set time
        }
        return ApplicationEvent.builder()
            .actor(entity.getActorName())
            .description(eventDescription(getMetadataRequest(entity), "instances", "memory", "state", "environment_json"))
            .event(entity.getType())
            .time(timestamp)
            .build();
    }

    private static Mono<Void> deleteRoute(final CloudFoundryClient cloudFoundryClient, String routeId) {
        return requestDeleteRoute(cloudFoundryClient, routeId)
            .map(ResourceUtils.extractId())
            .then(new Function<String, Mono<Void>>() {

                @Override
                public Mono<Void> apply(String jobId) {
                    return JobUtils.waitForCompletion(cloudFoundryClient, jobId);
                }

            });
    }

    private static Mono<Void> deleteRoutes(final CloudFoundryClient cloudFoundryClient, Optional<List<Route>> routes) {
        return routes
            .map(new Function<List<Route>, Stream<Route>>() {

                @Override
                public Stream<Route> apply(List<Route> routes) {
                    return Stream.fromIterable(routes);

                }
            })
            .orElse(Stream.<Route>empty())
            .map(new Function<Route, String>() {

                @Override
                public String apply(Route route) {
                    return route.getId();
                }

            })
            .flatMap(new Function<String, Mono<Void>>() {

                @Override
                public Mono<Void> apply(String routeId) {
                    return deleteRoute(cloudFoundryClient, routeId);
                }

            })
            .after();
    }

    private static String eventDescription(Map<String, Object> request, String... entryNames) {
        if (request == null) return "";
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String entryName : entryNames) {
            Object value = request.get(entryName);
            if (value == null) continue;
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(entryName).append(": ").append(String.valueOf(value));
        }
        return sb.toString();
    }

    private static Stream<SpaceApplicationSummary> extractApplications(GetSpaceSummaryResponse getSpaceSummaryResponse) {
        return Stream.fromIterable(getSpaceSummaryResponse.getApplications());
    }

    private static Mono<AbstractApplicationResource> getApplication(CloudFoundryClient cloudFoundryClient, String application, String spaceId) {
        return requestApplications(cloudFoundryClient, application, spaceId)
            .single()
            .otherwise(ExceptionUtils.<AbstractApplicationResource>convert("Application %s does not exist", application));
    }

    private static Mono<String> getApplicationId(CloudFoundryClient cloudFoundryClient, String application, String spaceId) {
        return getApplication(cloudFoundryClient, application, spaceId)
            .map(ResourceUtils.extractId());
    }

    private static Mono<String> getApplicationIdWhere(CloudFoundryClient cloudFoundryClient, String application, String spaceId, Predicate<AbstractApplicationResource> predicate) {
        return getApplication(cloudFoundryClient, application, spaceId)
            .where(predicate)
            .map(ResourceUtils.extractId());
    }

    private static Mono<Tuple4<ApplicationStatisticsResponse, SummaryApplicationResponse, GetStackResponse, ApplicationInstancesResponse>> getAuxiliaryContent(
        CloudFoundryClient cloudFoundryClient, AbstractApplicationResource applicationResource) {

        String applicationId = ResourceUtils.getId(applicationResource);
        String stackId = ResourceUtils.getEntity(applicationResource).getStackId();

        return Mono
            .when(
                requestApplicationStats(cloudFoundryClient, applicationId),
                requestApplicationSummary(cloudFoundryClient, applicationId),
                requestStack(cloudFoundryClient, stackId),
                requestApplicationInstances(cloudFoundryClient, applicationId)
            );
    }

    private static String getBuildpack(SummaryApplicationResponse response) {
        return Optional
            .ofNullable(response.getBuildpack())
            .orElse(response.getDetectedBuildpack());
    }

    private static Map<String, Object> getEnvironment(AbstractApplicationResource resource) {
        return ResourceUtils.getEntity(resource).getEnvironmentJsons();
    }

    private static Stream<EventResource> getEventResources(final String applicationId, final CloudFoundryClient cloudFoundryClient) {
        return PaginationUtils.requestResources(new Function<Integer, Mono<ListEventsResponse>>() {

            @Override
            public Mono<ListEventsResponse> apply(Integer page) {
                return cloudFoundryClient.events()
                    .list(ListEventsRequest.builder()
                        .actee(applicationId)
                        .orderDirection(PaginatedRequest.OrderDirection.DESC)
                        .resultsPerPage(50)
                        .page(page)
                        .build());
            }

        });
    }

    private static Publisher<LogMessage> getLogs(final LoggingClient loggingClient, final String applicationId, Boolean recent) {
        return Mono
            .just(Optional.ofNullable(recent).orElse(false))
            .where(new Predicate<Boolean>() {

                @Override
                public boolean test(Boolean recent) {
                    return recent;
                }

            })
            .flatMap(new Function<Boolean, Publisher<LogMessage>>() {

                @Override
                public Publisher<LogMessage> apply(Boolean recent) {
                    return loggingClient
                        .recent(RecentLogsRequest.builder()
                            .applicationId(applicationId)
                            .build());
                }

            })
            .switchIfEmpty(loggingClient
                .stream(StreamLogsRequest.builder()
                    .applicationId(applicationId)
                    .build()));
    }

    private static Map<String, Object> getMetadataRequest(EventEntity entity) {
        Map<String, Object> metadataMap = safeCastToMap(entity.getMetadatas());
        return (metadataMap == null) ? null : safeCastToMap(metadataMap.get("request"));
    }

    private static Mono<Optional<List<Route>>> getOptionalRoutes(final CloudFoundryClient cloudFoundryClient, boolean deleteRoutes, final String applicationId) {
        return Mono
            .just(deleteRoutes)
            .where(OperationUtils.identity())
            .then(new Function<Boolean, Mono<List<Route>>>() {

                @Override
                public Mono<List<Route>> apply(Boolean deleteRoutes) {
                    return getRoutes(cloudFoundryClient, applicationId);
                }

            })
            .map(OptionalUtils.<List<Route>>toOptional())
            .defaultIfEmpty(Optional.<List<Route>>empty());
    }

    private static Mono<List<Route>> getRoutes(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return requestApplicationSummary(cloudFoundryClient, applicationId)
            .map(new Function<SummaryApplicationResponse, List<Route>>() {

                @Override
                public List<Route> apply(SummaryApplicationResponse summaryApplicationResponse) {
                    return summaryApplicationResponse.getRoutes();
                }

            });
    }

    private static Mono<Tuple2<Optional<List<Route>>, String>> getRoutesAndApplicationId(final CloudFoundryClient cloudFoundryClient, final DeleteApplicationRequest deleteApplicationRequest,
                                                                                         String spaceId) {
        return getApplicationId(cloudFoundryClient, deleteApplicationRequest.getName(), spaceId)
            .then(new Function<String, Mono<Tuple2<Optional<List<Route>>, String>>>() {

                @Override
                public Mono<Tuple2<Optional<List<Route>>, String>> apply(final String applicationId) {
                    return getOptionalRoutes(cloudFoundryClient, deleteApplicationRequest.getDeleteRoutes(), applicationId)
                        .and(Mono.just(applicationId));
                }

            });
    }

    /**
     * Produces a Mono transformer that preserves the type of the source {@code Mono<IN>}.
     *
     * <p> The Mono produced expects a single element from the source, passes this to the predicate and, if this returns <b>{@code true}</b>, it builds a {@code Mono<IN>} with the {@code thenFunction}
     * and returns this. If the predicate returns <b>{@code false}</b> the input value is passed through unchanged.</p>
     *
     * <p> <b>Usage:</b> Can be used inline thus: {@code .as(ifThen(in -> test(in), in -> funcOf(in)))} </p>
     *
     * @param predicate    from source input element to <b>{@code true}</b> or <b>{@code false}</b>
     * @param thenFunction from source input element to some {@code Mono<IN>}
     * @param <IN>         the source element type and the element type of the resulting {@code Mono}.
     * @return a Mono transformer
     */
    private static <IN> Function<Mono<IN>, Mono<IN>> ifThen(final Predicate<IN> predicate, final Function<IN, Mono<IN>> thenFunction) {
        return new Function<Mono<IN>, Mono<IN>>() {

            @Override
            public Mono<IN> apply(Mono<IN> source) {
                return source
                    .then(new Function<IN, Mono<IN>>() {

                        @Override
                        public Mono<IN> apply(IN in) {
                            return predicate.test(in) ? thenFunction.apply(in) : Mono.just(in);
                        }

                    });
            }

        };
    }

    private static Predicate<AbstractApplicationResource> isIn(final String state) {
        return new Predicate<AbstractApplicationResource>() {

            @Override
            public boolean test(AbstractApplicationResource resource) {
                return state.equals(ResourceUtils.getEntity(resource).getState());
            }

        };
    }

    private static Predicate<String> isInstanceComplete() {
        return new Predicate<String>() {

            @Override
            public boolean test(String state) {
                return "RUNNING".equals(state) || "FAILED".equals(state);
            }

        };
    }

    private static boolean isRestartRequired(ScaleApplicationRequest request, AbstractApplicationResource applicationResource) {
        return (request.getDiskLimit() != null || request.getMemoryLimit() != null)
            && STARTED_STATE.equals(ResourceUtils.getEntity(applicationResource).getState());
    }

    private static Predicate<String> isRunning() {
        return new Predicate<String>() {

            @Override
            public boolean test(String state) {
                return "RUNNING".equals(state);
            }

        };
    }

    private static Predicate<String> isStaged() {
        return new Predicate<String>() {

            @Override
            public boolean test(String state) {
                return "STAGED".equals(state);
            }

        };
    }

    private static Predicate<String> isStagingComplete() {
        return new Predicate<String>() {

            @Override
            public boolean test(String state) {
                return "STAGED".equals(state) || "FAILED".equals(state);
            }

        };
    }

    private static Map<String, Object> removeFromEnvironment(Map<String, Object> environment, String variableName) {
        Map<String, Object> modified = new HashMap<>(environment);
        modified.remove(variableName);
        return modified;
    }

    private static Mono<ApplicationEnvironmentResponse> requestApplicationEnvironment(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .environment(ApplicationEnvironmentRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<ApplicationInstancesResponse> requestApplicationInstances(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .instances(ApplicationInstancesRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<ApplicationStatisticsResponse> requestApplicationStats(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .statistics(ApplicationStatisticsRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<SummaryApplicationResponse> requestApplicationSummary(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .summary(SummaryApplicationRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Stream<AbstractApplicationResource> requestApplications(final CloudFoundryClient cloudFoundryClient, final String application, final String spaceId) {
        return PaginationUtils
            .requestResources(new Function<Integer, Mono<ListSpaceApplicationsResponse>>() {

                @Override
                public Mono<ListSpaceApplicationsResponse> apply(Integer page) {
                    return cloudFoundryClient.spaces()
                        .listApplications(ListSpaceApplicationsRequest.builder()
                            .name(application)
                            .spaceId(spaceId)
                            .page(page)
                            .build());
                }

            })
            .map(OperationUtils.<ApplicationResource, AbstractApplicationResource>cast());
    }

    private static Mono<Void> requestDeleteApplication(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .delete(org.cloudfoundry.client.v2.applications.DeleteApplicationRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<DeleteRouteResponse> requestDeleteRoute(CloudFoundryClient cloudFoundryClient, String routeId) {
        return cloudFoundryClient.routes()
            .delete(DeleteRouteRequest.builder()
                .async(true)
                .routeId(routeId)
                .build());
    }

    private static Mono<GetApplicationResponse> requestGetApplication(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .get(org.cloudfoundry.client.v2.applications.GetApplicationRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<RestageApplicationResponse> requestRestageApplication(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return cloudFoundryClient.applicationsV2()
            .restage(org.cloudfoundry.client.v2.applications.RestageApplicationRequest.builder()
                .applicationId(applicationId)
                .build());
    }

    private static Mono<GetSpaceSummaryResponse> requestSpaceSummary(CloudFoundryClient cloudFoundryClient, String spaceId) {
        return cloudFoundryClient.spaces()
            .getSummary(GetSpaceSummaryRequest.builder()
                .spaceId(spaceId)
                .build());
    }

    private static Mono<GetStackResponse> requestStack(CloudFoundryClient cloudFoundryClient, String stackId) {
        return cloudFoundryClient.stacks()
            .get(GetStackRequest.builder()
                .stackId(stackId)
                .build());
    }

    private static Mono<String> requestStackName(CloudFoundryClient cloudFoundryClient, String stackId) {
        return requestStack(cloudFoundryClient, stackId)
            .map(new Function<GetStackResponse, String>() {

                @Override
                public String apply(GetStackResponse getStackResponse) {
                    return getStackResponse.getEntity().getName();
                }

            });
    }

    private static Mono<Void> requestTerminateApplicationInstance(CloudFoundryClient cloudFoundryClient, String applicationId, String instanceIndex) {
        return cloudFoundryClient.applicationsV2()
            .terminateInstance(TerminateApplicationInstanceRequest.builder()
                .applicationId(applicationId)
                .index(instanceIndex)
                .build());
    }

    private static Mono<UpdateApplicationResponse> requestUpdateApplicationEnvironment(CloudFoundryClient cloudFoundryClient, String applicationId, Map<String, Object> environment) {
        return cloudFoundryClient.applicationsV2()
            .update(UpdateApplicationRequest.builder()
                .applicationId(applicationId)
                .environmentJsons(environment)
                .build());
    }

    private static Mono<UpdateApplicationResponse> requestUpdateApplicationRename(CloudFoundryClient cloudFoundryClient, String applicationId, String name) {
        return cloudFoundryClient.applicationsV2()
            .update(UpdateApplicationRequest.builder()
                .applicationId(applicationId)
                .name(name)
                .build());
    }

    private static Mono<AbstractApplicationResource> requestUpdateApplicationScale(CloudFoundryClient cloudFoundryClient, String applicationId, Integer disk, Integer instances, Integer memory) {
        return cloudFoundryClient.applicationsV2()
            .update(UpdateApplicationRequest.builder()
                .applicationId(applicationId)
                .diskQuota(disk)
                .instances(instances)
                .memory(memory)
                .build())
            .map(OperationUtils.<UpdateApplicationResponse, AbstractApplicationResource>cast());
    }

    private static Mono<AbstractApplicationResource> requestUpdateApplicationSsh(CloudFoundryClient cloudFoundryClient, String applicationId, Boolean enabled) {
        return cloudFoundryClient.applicationsV2()
            .update(UpdateApplicationRequest.builder()
                .applicationId(applicationId)
                .enableSsh(enabled)
                .build())
            .map(OperationUtils.<UpdateApplicationResponse, AbstractApplicationResource>cast());
    }

    private static Mono<AbstractApplicationResource> requestUpdateApplicationState(CloudFoundryClient cloudFoundryClient, String applicationId, String state) {
        return cloudFoundryClient.applicationsV2()
            .update(UpdateApplicationRequest.builder()
                .applicationId(applicationId)
                .state(state)
                .build())
            .map(OperationUtils.<UpdateApplicationResponse, AbstractApplicationResource>cast());
    }

    private static Mono<String> restageApplication(final CloudFoundryClient cloudFoundryClient, final String application, final String applicationId) {
        return requestRestageApplication(cloudFoundryClient, applicationId)
            .then(new Function<RestageApplicationResponse, Mono<String>>() {

                @Override
                public Mono<String> apply(RestageApplicationResponse response) {
                    return waitForStaging(cloudFoundryClient, application, applicationId);
                }

            })
            .then(new Function<String, Mono<String>>() {

                @Override
                public Mono<String> apply(String state) {
                    return waitForRunning(cloudFoundryClient, application, applicationId);
                }

            });
    }

    private static Mono<String> restartApplication(final CloudFoundryClient cloudFoundryClient, final String application, final AbstractApplicationResource resource) {
        return stopApplication(cloudFoundryClient, ResourceUtils.getId(resource))
            .then(new Function<AbstractApplicationResource, Mono<String>>() {

                @Override
                public Mono<String> apply(AbstractApplicationResource abstractApplicationResource) {
                    return startApplicationAndWait(cloudFoundryClient, application, ResourceUtils.getId(resource));
                }

            });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> safeCastToMap(Object request) {
        if (request instanceof Map)
            return (Map<String, Object>) request;
        else
            return null;
    }

    private static Predicate<AbstractApplicationResource> sshEnabled(final Boolean enabled) {
        return new Predicate<AbstractApplicationResource>() {

            @Override
            public boolean test(AbstractApplicationResource resource) {
                return enabled.equals(ResourceUtils.getEntity(resource).getEnableSsh());
            }

        };
    }

    private static Mono<String> startApplicationAndWait(final CloudFoundryClient cloudFoundryClient, final String application, final String applicationId) {
        return requestUpdateApplicationState(cloudFoundryClient, applicationId, STARTED_STATE)
            .then(new Function<AbstractApplicationResource, Mono<String>>() {

                @Override
                public Mono<String> apply(AbstractApplicationResource resource) {
                    return waitForRunning(cloudFoundryClient, application, applicationId);
                }

            });
    }

    private static Mono<AbstractApplicationResource> stopApplication(CloudFoundryClient cloudFoundryClient, String applicationId) {
        return requestUpdateApplicationState(cloudFoundryClient, applicationId, STOPPED_STATE);
    }

    private static ApplicationDetail toApplicationDetail(ApplicationStatisticsResponse applicationStatisticsResponse, SummaryApplicationResponse summaryApplicationResponse,
                                                         GetStackResponse getStackResponse, ApplicationInstancesResponse applicationInstancesResponse) {

        return ApplicationDetail.builder()
            .buildpack(getBuildpack(summaryApplicationResponse))
            .diskQuota(summaryApplicationResponse.getDiskQuota())
            .id(summaryApplicationResponse.getId())
            .instanceDetails(toInstanceDetailList(applicationInstancesResponse, applicationStatisticsResponse))
            .instances(summaryApplicationResponse.getInstances())
            .lastUploaded(toDate(summaryApplicationResponse.getPackageUpdatedAt()))
            .memoryLimit(summaryApplicationResponse.getMemory())
            .name(summaryApplicationResponse.getName())
            .requestedState(summaryApplicationResponse.getState())
            .runningInstances(summaryApplicationResponse.getRunningInstances())
            .stack(getStackResponse.getEntity().getName())
            .urls(toUrls(summaryApplicationResponse.getRoutes()))
            .build();
    }

    private static ApplicationEnvironments toApplicationEnvironments(ApplicationEnvironmentResponse response) {
        return ApplicationEnvironments.builder()
            .running(response.getRunningEnvironmentJsons())
            .staging(response.getStagingEnvironmentJsons())
            .systemProvided(response.getSystemEnvironmentJsons())
            .userProvided(response.getEnvironmentJsons())
            .build();
    }

    private static Mono<ApplicationManifest> toApplicationManifest(SummaryApplicationResponse summaryApplicationResponse, String stackName) {
        ApplicationManifest.ApplicationManifestBuilder manifestBuilder = ApplicationManifest.builder()
            .buildpack(summaryApplicationResponse.getBuildpack())
            .command(summaryApplicationResponse.getCommand())
            .diskQuotaMB(summaryApplicationResponse.getDiskQuota())
            .envs(summaryApplicationResponse.getEnvironmentJsons())
            .instances(summaryApplicationResponse.getInstances())
            .memoryMB(summaryApplicationResponse.getMemory())
            .name(summaryApplicationResponse.getName())
            .stack(stackName)
            .timeout(summaryApplicationResponse.getHealthCheckTimeout());

        for (Route route : summaryApplicationResponse.getRoutes()) {
            manifestBuilder
                .domain(route.getDomain().getName())
                .host((route.getHost()));
        }

        for (ServiceInstance service : summaryApplicationResponse.getServices()) {
            manifestBuilder
                .service(service.getName());
        }

        return Mono.just(manifestBuilder
            .build());
    }

    private static ApplicationSummary toApplicationSummary(SpaceApplicationSummary spaceApplicationSummary) {
        return ApplicationSummary.builder()
            .diskQuota(spaceApplicationSummary.getDiskQuota())
            .id(spaceApplicationSummary.getId())
            .instances(spaceApplicationSummary.getInstances())
            .memoryLimit(spaceApplicationSummary.getMemory())
            .name(spaceApplicationSummary.getName())
            .requestedState(spaceApplicationSummary.getState())
            .runningInstances(spaceApplicationSummary.getRunningInstances())
            .urls(spaceApplicationSummary.getUrls())
            .build();
    }

    private static Date toDate(String date) {
        return date == null ? null : parseFromIso8601(date);
    }

    private static Date toDate(Double date) {
        return date == null ? null : DateUtils.parseSecondsFromEpoch(date);
    }

    private static ApplicationDetail.InstanceDetail toInstanceDetail(Map.Entry<String, ApplicationInstanceInfo> entry, ApplicationStatisticsResponse statisticsResponse) {
        ApplicationStatisticsResponse.InstanceStats.Statistics stats = statisticsResponse.get(entry.getKey()).getStatistics();
        ApplicationStatisticsResponse.InstanceStats.Statistics.Usage usage = stats.getUsage();

        return ApplicationDetail.InstanceDetail.builder()
            .state(entry.getValue().getState())
            .since(toDate(entry.getValue().getSince()))
            .cpu(usage.getCpu())
            .memoryUsage(usage.getMemory())
            .diskUsage(usage.getDisk())
            .diskQuota(stats.getDiskQuota())
            .memoryQuota(stats.getMemoryQuota())
            .build();
    }

    private static List<ApplicationDetail.InstanceDetail> toInstanceDetailList(ApplicationInstancesResponse instancesResponse, final ApplicationStatisticsResponse statisticsResponse) {
        return Stream
            .fromIterable(instancesResponse.entrySet())
            .map(new Function<Map.Entry<String, ApplicationInstanceInfo>, ApplicationDetail.InstanceDetail>() {

                @Override
                public ApplicationDetail.InstanceDetail apply(Map.Entry<String, ApplicationInstanceInfo> entry) {
                    return toInstanceDetail(entry, statisticsResponse);
                }

            })
            .toList()
            .get();
    }

    private static String toUrl(Route route) {
        String hostName = route.getHost();
        String domainName = route.getDomain().getName();

        return hostName.isEmpty() ? domainName : String.format("%s.%s", hostName, domainName);
    }

    private static List<String> toUrls(List<Route> routes) {
        return Stream
            .fromIterable(routes)
            .map(new Function<Route, String>() {

                @Override
                public String apply(Route route) {
                    return toUrl(route);
                }

            })
            .toList()
            .get();
    }

    private static Mono<String> waitForRunning(CloudFoundryClient cloudFoundryClient, String application, String applicationId) {
        return requestApplicationInstances(cloudFoundryClient, applicationId)
            .flatMap(new Function<ApplicationInstancesResponse, Stream<ApplicationInstanceInfo>>() {

                @Override
                public Stream<ApplicationInstanceInfo> apply(ApplicationInstancesResponse response) {
                    return Stream.fromIterable(response.values());
                }
            })
            .map(new Function<ApplicationInstanceInfo, String>() {

                @Override
                public String apply(ApplicationInstanceInfo applicationInstanceInfo) {
                    return applicationInstanceInfo.getState();
                }
            })
            .as(OperationUtils.<String>stream())
            .reduce("UNKNOWN", collectStates())
            .where(isInstanceComplete())
            .as(OperationUtils.<String>repeatWhen(DelayUtils.exponentialBackOff(1, 10, SECONDS, 10)))  // TODO: Remove once Mono.repeatWhen()
            .where(isRunning())
            .otherwiseIfEmpty(ExceptionUtils.<String>illegalState("Application %s failed during start", application));
    }

    private static Mono<String> waitForStaging(CloudFoundryClient cloudFoundryClient, String application, String applicationId) {
        return requestGetApplication(cloudFoundryClient, applicationId)
            .map(new Function<GetApplicationResponse, String>() {

                @Override
                public String apply(GetApplicationResponse response) {
                    return ResourceUtils.getEntity(response).getPackageState();
                }

            })
            .where(isStagingComplete())
            .as(OperationUtils.<String>repeatWhen(DelayUtils.exponentialBackOff(1, 10, SECONDS, 10)))  // TODO: Remove once Mono.repeatWhen()
            .where(isStaged())
            .otherwiseIfEmpty(ExceptionUtils.<String>illegalState("Application %s failed during staging", application));
    }

}
