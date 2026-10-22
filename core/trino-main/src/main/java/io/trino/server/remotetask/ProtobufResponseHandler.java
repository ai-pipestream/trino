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

import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.trino.server.protobuf.ProtobufMediaTypes.APPLICATION_X_PROTOBUF;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Reads a protobuf response body, mirroring {@code FullJsonResponseHandler}:
 * protocol errors are captured in the response instead of thrown, so callers
 * can apply the same error-tracking logic as for JSON. A successful response
 * whose content type is not {@code application/x-protobuf} is an error: with
 * the control-plane switch set to PROTOBUF there is no JSON fallback.
 */
public class ProtobufResponseHandler<T>
        implements ResponseHandler<ProtobufResponseHandler.ProtobufResponse<T>, RuntimeException>
{
    private final Function<byte[], T> parser;

    public ProtobufResponseHandler(Function<byte[], T> parser)
    {
        this.parser = requireNonNull(parser, "parser is null");
    }

    public static <T> ProtobufResponseHandler<T> createProtobufResponseHandler(Function<byte[], T> parser)
    {
        return new ProtobufResponseHandler<>(parser);
    }

    @Override
    public ProtobufResponse<T> handleException(Request request, Exception exception)
    {
        throw propagate(request, exception);
    }

    @Override
    public ProtobufResponse<T> handle(Request request, Response response)
    {
        byte[] body;
        try {
            body = response.getInputStream().readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int statusCode = response.getStatusCode();
        if (statusCode != HttpStatus.OK.code() || body.length == 0) {
            return new ProtobufResponse<>(statusCode, null, null, new String(body, UTF_8), body.length);
        }
        Optional<String> contentType = response.getHeader(CONTENT_TYPE);
        if (contentType.isEmpty() || !contentType.get().startsWith(APPLICATION_X_PROTOBUF)) {
            return new ProtobufResponse<>(
                    statusCode,
                    null,
                    new IllegalStateException("Expected " + APPLICATION_X_PROTOBUF + " response from " + request.getUri() + " but got " + contentType.orElse("no content type")),
                    new String(body, UTF_8),
                    body.length);
        }
        try {
            return new ProtobufResponse<>(statusCode, parser.apply(body), null, "", body.length);
        }
        catch (RuntimeException e) {
            return new ProtobufResponse<>(statusCode, null, e, new String(body, UTF_8), body.length);
        }
    }

    public static class ProtobufResponse<T>
    {
        private final int statusCode;
        @Nullable
        private final T value;
        @Nullable
        private final Exception exception;
        private final String responseBody;
        private final long responseSize;

        public ProtobufResponse(int statusCode, @Nullable T value, @Nullable Exception exception, String responseBody, long responseSize)
        {
            this.statusCode = statusCode;
            this.value = value;
            this.exception = exception;
            this.responseBody = requireNonNull(responseBody, "responseBody is null");
            this.responseSize = responseSize;
        }

        public int getStatusCode()
        {
            return statusCode;
        }

        public boolean hasValue()
        {
            return value != null;
        }

        public T getValue()
        {
            if (value == null) {
                throw new IllegalStateException("Response does not have a value");
            }
            return value;
        }

        @Nullable
        public Exception getException()
        {
            return exception;
        }

        public String getResponseBody()
        {
            return responseBody;
        }

        public long getResponseSize()
        {
            return responseSize;
        }
    }
}
