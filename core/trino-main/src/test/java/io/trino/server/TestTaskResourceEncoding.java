/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.client.QueryResults;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.QueryInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TaskStatus;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.protobuf.DynamicFilterDomainsProtoMapper;
import io.trino.server.protobuf.FailTaskRequestProtoMapper;
import io.trino.server.protobuf.TaskStatusProtoMapper;
import io.trino.server.remotetask.ProtobufResponseHandler.ProtobufResponse;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;

import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.TestQueryResource.QUERY_RESULTS_JSON_CODEC;
import static io.trino.server.TestQueryResource.REQUEST_USER_HEADER;
import static io.trino.server.protobuf.ProtobufMediaTypes.APPLICATION_X_PROTOBUF;
import static io.trino.server.remotetask.ProtobufResponseHandler.createProtobufResponseHandler;
import static io.trino.util.Failures.toFailure;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The task control plane answers in the configured encoding only. There is no
 * format negotiation: under PROTOBUF a request that only accepts JSON gets 406
 * and a JSON body gets 415, and under JSON the protobuf forms are rejected the
 * same way. The positive cases assert the wire content type end to end against
 * a task that ran on the server.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestTaskResourceEncoding
{
    private static final JsonCodec<FailTaskRequest> FAIL_TASK_REQUEST_CODEC = JsonCodec.jsonCodec(FailTaskRequest.class);

    private HttpClient client;
    private TestingTrinoServer protobufServer;
    private TestingTrinoServer jsonServer;
    private URI protobufTask;
    private URI jsonTask;

    @BeforeAll
    public void setup()
    {
        client = new JettyHttpClient();
        protobufServer = createServer(ControlPlaneEncoding.PROTOBUF);
        jsonServer = createServer(ControlPlaneEncoding.JSON);
        protobufTask = runQueryAndGetTaskUri(protobufServer);
        jsonTask = runQueryAndGetTaskUri(jsonServer);
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(protobufServer, jsonServer, client);
        protobufServer = null;
        jsonServer = null;
        client = null;
    }

    @Test
    public void testIsAcceptable()
    {
        assertThat(TaskResource.isAcceptable(null, ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("*/*", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("application/*", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("application/x-protobuf", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("application/json, application/x-protobuf;q=0.5", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("application/json", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("application/x-protobuf;q=0", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("text/plain, not a media type", ControlPlaneEncoding.PROTOBUF)).isFalse();
        // a zero weight may carry up to three decimals
        assertThat(TaskResource.isAcceptable("application/x-protobuf;q=0.0", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("application/x-protobuf; q=0.000", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("application/x-protobuf;q=0.001", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("application/x-protobuf;q=abc", ControlPlaneEncoding.PROTOBUF)).isFalse();
        // the most specific matching range decides, whatever the order
        assertThat(TaskResource.isAcceptable("*/*, application/x-protobuf;q=0", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("application/x-protobuf;q=0, application/*", ControlPlaneEncoding.PROTOBUF)).isFalse();
        assertThat(TaskResource.isAcceptable("*/*;q=0, application/x-protobuf", ControlPlaneEncoding.PROTOBUF)).isTrue();
        assertThat(TaskResource.isAcceptable("*/*;q=0", ControlPlaneEncoding.PROTOBUF)).isFalse();

        assertThat(TaskResource.isAcceptable("application/json; charset=utf-8", ControlPlaneEncoding.JSON)).isTrue();
        assertThat(TaskResource.isAcceptable("application/x-protobuf", ControlPlaneEncoding.JSON)).isFalse();
    }

    @Test
    public void testProtobufServerAnswersInProtobuf()
    {
        ProtobufResponse<TaskStatus> status = client.execute(
                internal(protobufServer, prepareGet()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("status").build())
                        .setHeader(ACCEPT, APPLICATION_X_PROTOBUF)),
                createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom));
        assertThat(status.getStatusCode()).isEqualTo(200);
        assertThat(status.hasValue()).isTrue();
        assertThat(status.getValue().self()).isEqualTo(protobufTask);

        DynamicFilterDomainsProtoMapper mapper = protobufServer.getInstance(Key.get(DynamicFilterDomainsProtoMapper.class));
        ProtobufResponse<VersionedDynamicFilterDomains> dynamicFilters = client.execute(
                internal(protobufServer, prepareGet()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("dynamicfilters").build())
                        .setHeader(ACCEPT, APPLICATION_X_PROTOBUF)
                        .setHeader(TRINO_CURRENT_VERSION, "0")),
                createProtobufResponseHandler(mapper::parseFrom));
        assertThat(dynamicFilters.getStatusCode()).isEqualTo(200);
        assertThat(dynamicFilters.hasValue()).isTrue();

        // the protobuf response handler is the production one and refuses anything but application/x-protobuf,
        // so a 200 with a value means the content type was right; check the header directly as well
        StatusResponse raw = client.execute(
                internal(protobufServer, prepareGet()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("status").build())),
                createStatusResponseHandler());
        assertThat(raw.getStatusCode()).isEqualTo(200);
        assertThat(raw.getHeader(CONTENT_TYPE)).contains(APPLICATION_X_PROTOBUF);
    }

    @Test
    public void testProtobufServerRejectsJson()
    {
        StatusResponse status = client.execute(
                internal(protobufServer, prepareGet()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("status").build())
                        .setHeader(ACCEPT, APPLICATION_JSON)),
                createStatusResponseHandler());
        assertThat(status.getStatusCode()).isEqualTo(406);

        StatusResponse dynamicFilters = client.execute(
                internal(protobufServer, prepareGet()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("dynamicfilters").build())
                        .setHeader(ACCEPT, APPLICATION_JSON)
                        .setHeader(TRINO_CURRENT_VERSION, "0")),
                createStatusResponseHandler());
        assertThat(dynamicFilters.getStatusCode()).isEqualTo(406);

        StatusResponse fail = client.execute(
                internal(protobufServer, preparePost()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("fail").build())
                        .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                        .setBodyGenerator(createStaticBodyGenerator(FAIL_TASK_REQUEST_CODEC.toJsonBytes(failTaskRequest())))),
                createStatusResponseHandler());
        assertThat(fail.getStatusCode()).isEqualTo(415);
    }

    @Test
    public void testProtobufServerFailsTaskFromProtobuf()
    {
        StatusResponse fail = client.execute(
                internal(protobufServer, preparePost()
                        .setUri(uriBuilderFrom(protobufTask).appendPath("fail").build())
                        .setHeader(CONTENT_TYPE, APPLICATION_X_PROTOBUF)
                        .setHeader(ACCEPT, APPLICATION_JSON)
                        .setBodyGenerator(createStaticBodyGenerator(FailTaskRequestProtoMapper.toByteArray(failTaskRequest())))),
                createStatusResponseHandler());
        assertThat(fail.getStatusCode()).isEqualTo(200);
        // TaskInfo has no protobuf schema yet, so the response stays JSON
        assertThat(fail.getHeader(CONTENT_TYPE)).contains(APPLICATION_JSON);
    }

    @Test
    public void testJsonServerAnswersInJson()
    {
        StatusResponse status = client.execute(
                internal(jsonServer, prepareGet()
                        .setUri(uriBuilderFrom(jsonTask).appendPath("status").build())
                        .setHeader(ACCEPT, APPLICATION_JSON)),
                createStatusResponseHandler());
        assertThat(status.getStatusCode()).isEqualTo(200);
        assertThat(status.getHeader(CONTENT_TYPE)).contains(APPLICATION_JSON);

        StatusResponse dynamicFilters = client.execute(
                internal(jsonServer, prepareGet()
                        .setUri(uriBuilderFrom(jsonTask).appendPath("dynamicfilters").build())
                        .setHeader(TRINO_CURRENT_VERSION, "0")),
                createStatusResponseHandler());
        assertThat(dynamicFilters.getStatusCode()).isEqualTo(200);
        assertThat(dynamicFilters.getHeader(CONTENT_TYPE)).contains(APPLICATION_JSON);
    }

    @Test
    public void testJsonServerRejectsProtobuf()
    {
        StatusResponse status = client.execute(
                internal(jsonServer, prepareGet()
                        .setUri(uriBuilderFrom(jsonTask).appendPath("status").build())
                        .setHeader(ACCEPT, APPLICATION_X_PROTOBUF)),
                createStatusResponseHandler());
        assertThat(status.getStatusCode()).isEqualTo(406);

        StatusResponse dynamicFilters = client.execute(
                internal(jsonServer, prepareGet()
                        .setUri(uriBuilderFrom(jsonTask).appendPath("dynamicfilters").build())
                        .setHeader(ACCEPT, APPLICATION_X_PROTOBUF)
                        .setHeader(TRINO_CURRENT_VERSION, "0")),
                createStatusResponseHandler());
        assertThat(dynamicFilters.getStatusCode()).isEqualTo(406);

        StatusResponse fail = client.execute(
                internal(jsonServer, preparePost()
                        .setUri(uriBuilderFrom(jsonTask).appendPath("fail").build())
                        .setHeader(CONTENT_TYPE, APPLICATION_X_PROTOBUF)
                        .setBodyGenerator(createStaticBodyGenerator(FailTaskRequestProtoMapper.toByteArray(failTaskRequest())))),
                createStatusResponseHandler());
        assertThat(fail.getStatusCode()).isEqualTo(415);
    }

    private static TestingTrinoServer createServer(ControlPlaneEncoding encoding)
    {
        TestingTrinoServer server = TestingTrinoServer.builder()
                .addProperty("internal-communication.control-plane.encoding", encoding.name())
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        return server;
    }

    private URI runQueryAndGetTaskUri(TestingTrinoServer server)
    {
        Request request = preparePost()
                .setHeader(REQUEST_USER_HEADER, "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                .setBodyGenerator(createStaticBodyGenerator("SELECT count(*) FROM tpch.tiny.nation", UTF_8))
                .build();
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(
                    prepareGet()
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .setUri(queryResults.getNextUri())
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        }
        assertThat(queryResults.getError()).isNull();

        QueryInfo queryInfo = server.getQueryManager().getFullQueryInfo(new QueryId(queryResults.getId()));
        StagesInfo stages = queryInfo.getStages().orElseThrow();
        // finished tasks stay in the task manager for task.info.max-age, so the control plane endpoints still serve them
        return stages.getStages().stream()
                .flatMap(stage -> stage.tasks().stream())
                .map(task -> task.taskStatus().self())
                .findFirst()
                .orElseThrow();
    }

    private static Request internal(TestingTrinoServer server, Request.Builder request)
    {
        return server.getInstance(Key.get(InternalAuthenticationManager.class)).filterRequest(request.build());
    }

    private static FailTaskRequest failTaskRequest()
    {
        return new FailTaskRequest(toFailure(new RuntimeException("failed by test")));
    }
}
