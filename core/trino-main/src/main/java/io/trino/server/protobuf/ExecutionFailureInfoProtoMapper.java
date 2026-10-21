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

import io.trino.client.ErrorLocation;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.HostAddress;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class ExecutionFailureInfoProtoMapper
{
    private ExecutionFailureInfoProtoMapper() {}

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
                                io.trino.spi.ErrorType.valueOf(proto.getErrorCode().getType().name()),
                                proto.getErrorCode().getFatal())
                        : null,
                proto.hasRemoteHost() ? HostAddress.fromString(proto.getRemoteHost()) : null);
    }
}
