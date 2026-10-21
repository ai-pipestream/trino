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

import com.google.protobuf.InvalidProtocolBufferException;
import io.trino.server.FailTaskRequest;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class FailTaskRequestProtoMapper
{
    private FailTaskRequestProtoMapper() {}

    public static io.trino.server.protocol.v1.FailTaskRequest toProto(FailTaskRequest request)
    {
        return io.trino.server.protocol.v1.FailTaskRequest.newBuilder()
                .setFailureInfo(ExecutionFailureInfoProtoMapper.toProto(request.getFailureInfo()))
                .build();
    }

    public static FailTaskRequest fromProto(io.trino.server.protocol.v1.FailTaskRequest proto)
    {
        return new FailTaskRequest(ExecutionFailureInfoProtoMapper.fromProto(proto.getFailureInfo()));
    }

    public static byte[] toByteArray(FailTaskRequest request)
    {
        return toProto(request).toByteArray();
    }

    public static FailTaskRequest parseFrom(byte[] bytes)
    {
        try {
            return fromProto(io.trino.server.protocol.v1.FailTaskRequest.parseFrom(bytes));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf FailTaskRequest", e);
        }
    }
}
