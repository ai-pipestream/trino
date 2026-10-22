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
package io.trino.server.remotetask;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpVersion;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.server.protobuf.TaskStatusProtoMapper;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.server.protobuf.ProtobufMediaTypes.APPLICATION_X_PROTOBUF;
import static io.trino.server.remotetask.ProtobufResponseHandler.createProtobufResponseHandler;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestProtobufResponseHandler
{
    private static final URI TASK_URI = URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0/status");

    @Test
    public void testProtobufResponseIsDecoded()
    {
        TaskStatus taskStatus = initialTaskStatus();
        byte[] body = TaskStatusProtoMapper.toByteArray(taskStatus);
        assertThat(body[0]).isNotEqualTo((byte) '{');

        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, APPLICATION_X_PROTOBUF), body));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.code());
        assertThat(response.hasValue()).isTrue();
        assertThat(response.getValue().taskId()).isEqualTo(taskStatus.taskId());
        assertThat(response.getException()).isNull();
    }

    @Test
    public void testJsonResponseIsRejectedWithoutFallback()
    {
        byte[] jsonBody = "{\"taskId\":\"20240101_000000_00000_xyz.0.0.0\"}".getBytes(UTF_8);
        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "application/json"), jsonBody));
        assertThat(response.hasValue()).isFalse();
        assertThat(response.getException()).isNotNull();
        assertThat(response.getException().getMessage()).contains(APPLICATION_X_PROTOBUF);
    }

    @Test
    public void testMissingContentTypeIsRejected()
    {
        byte[] body = TaskStatusProtoMapper.toByteArray(initialTaskStatus());
        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.OK, ImmutableListMultimap.of(), body));
        assertThat(response.hasValue()).isFalse();
        assertThat(response.getException()).isNotNull();
        assertThat(response.getException().getMessage()).contains("no content type");
    }

    @Test
    public void testNonOkStatusHasNoValueAndNoException()
    {
        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.SERVICE_UNAVAILABLE, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), "busy".getBytes(UTF_8)));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE.code());
        assertThat(response.hasValue()).isFalse();
        assertThat(response.getException()).isNull();
        assertThat(response.getResponseBody()).isEqualTo("busy");
        assertThat(response.getResponseSize()).isEqualTo(4);
    }

    @Test
    public void testEmptyOkBodyHasNoValue()
    {
        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, APPLICATION_X_PROTOBUF), new byte[0]));
        assertThat(response.hasValue()).isFalse();
        assertThat(response.getException()).isNull();
    }

    @Test
    public void testCorruptBodyIsCapturedAsException()
    {
        byte[] truncated = {0x0A, 0x05, 0x01};
        ProtobufResponseHandler.ProtobufResponse<TaskStatus> response = createProtobufResponseHandler(TaskStatusProtoMapper::parseFrom)
                .handle(request(), new TestResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, APPLICATION_X_PROTOBUF), truncated));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.code());
        assertThat(response.hasValue()).isFalse();
        assertThat(response.getException()).isInstanceOf(TrinoException.class);
    }

    private static TaskStatus initialTaskStatus()
    {
        return TaskStatus.initialTaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.0.0.0"),
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0"),
                "worker-0",
                false);
    }

    private static Request request()
    {
        return prepareGet().setUri(TASK_URI).build();
    }

    private record TestResponse(HttpStatus status, ListMultimap<HeaderName, String> headers, byte[] body)
            implements Response
    {
        @Override
        public HttpVersion getHttpVersion()
        {
            return HttpVersion.HTTP_1;
        }

        @Override
        public int getStatusCode()
        {
            return status.code();
        }

        @Override
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public Content getContent()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream()
        {
            return new ByteArrayInputStream(body);
        }

        @Override
        public long getBytesRead()
        {
            return body.length;
        }
    }
}
