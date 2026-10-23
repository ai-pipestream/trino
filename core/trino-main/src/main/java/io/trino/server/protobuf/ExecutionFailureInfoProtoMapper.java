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
package io.trino.server.protobuf;

import com.google.protobuf.CodedInputStream;
import io.trino.client.ErrorLocation;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class ExecutionFailureInfoProtoMapper
{
    // Failures.toFailure does not bound the cause chain, so allow the nesting
    // depth the JSON codec accepts (Jackson's default) instead of protobuf's 100
    private static final int MAX_NESTING_DEPTH = 1000;

    private ExecutionFailureInfoProtoMapper() {}

    /**
     * Input for messages that embed {@code ExecutionFailureInfo}.
     */
    static CodedInputStream newCodedInputStream(byte[] bytes)
    {
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        input.setRecursionLimit(MAX_NESTING_DEPTH);
        return input;
    }

    public static io.trino.server.protocol.v1.ExecutionFailureInfo toProto(ExecutionFailureInfo failureInfo)
    {
        io.trino.server.protocol.v1.ExecutionFailureInfo.Builder builder = io.trino.server.protocol.v1.ExecutionFailureInfo.newBuilder()
                .setType(failureInfo.type())
                .addAllStack(failureInfo.stack());
        if (failureInfo.message() != null) {
            builder.setMessage(failureInfo.message());
        }
        if (failureInfo.cause() != null) {
            builder.setCause(toProto(failureInfo.cause()));
        }
        failureInfo.suppressed().forEach(suppressed -> builder.addSuppressed(toProto(suppressed)));
        if (failureInfo.errorLocation() != null) {
            builder.setErrorLocation(io.trino.server.protocol.v1.ErrorLocation.newBuilder()
                    .setLineNumber(failureInfo.errorLocation().getLineNumber())
                    .setColumnNumber(failureInfo.errorLocation().getColumnNumber())
                    .build());
        }
        if (failureInfo.errorCode() != null) {
            ErrorCode errorCode = failureInfo.errorCode();
            builder.setErrorCode(io.trino.server.protocol.v1.ErrorCode.newBuilder()
                    .setCode(errorCode.getCode())
                    .setName(errorCode.getName())
                    .setType(io.trino.server.protocol.v1.ErrorType.valueOf(errorCode.getType().name()))
                    .setFatal(errorCode.isFatal())
                    .build());
        }
        if (failureInfo.remoteHost() != null) {
            builder.setRemoteHost(failureInfo.remoteHost().toString());
        }
        return builder.build();
    }

    public static ExecutionFailureInfo fromProto(io.trino.server.protocol.v1.ExecutionFailureInfo proto)
    {
        return new ExecutionFailureInfo(
                proto.getType(),
                proto.hasMessage() ? proto.getMessage() : null,
                proto.hasCause() ? fromProto(proto.getCause()) : null,
                proto.getSuppressedList().stream()
                        .map(ExecutionFailureInfoProtoMapper::fromProto)
                        .collect(toImmutableList()),
                proto.getStackList(),
                proto.hasErrorLocation()
                        ? new ErrorLocation(proto.getErrorLocation().getLineNumber(), proto.getErrorLocation().getColumnNumber())
                        : null,
                proto.hasErrorCode()
                        ? new ErrorCode(
                        proto.getErrorCode().getCode(),
                        proto.getErrorCode().getName(),
                        errorType(proto.getErrorCode()),
                        proto.getErrorCode().getFatal())
                        : null,
                proto.hasRemoteHost() ? HostAddress.fromString(proto.getRemoteHost()) : null);
    }

    private static io.trino.spi.ErrorType errorType(io.trino.server.protocol.v1.ErrorCode errorCode)
    {
        io.trino.server.protocol.v1.ErrorType errorType = errorCode.getType();
        // UNRECOGNIZED has no number, so report the raw wire value
        if (errorType == io.trino.server.protocol.v1.ErrorType.ERROR_TYPE_UNSPECIFIED || errorType == io.trino.server.protocol.v1.ErrorType.UNRECOGNIZED) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid error type in protobuf ExecutionFailureInfo: " + errorCode.getTypeValue());
        }
        return io.trino.spi.ErrorType.valueOf(errorType.name());
    }
}
