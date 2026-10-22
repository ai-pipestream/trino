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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.protobuf.ExecutionFailureInfoProtoMapper.newCodedInputStream;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class TaskStatusProtoMapper
{
    private TaskStatusProtoMapper() {}

    public static io.trino.server.protocol.v1.TaskStatus toProto(TaskStatus taskStatus)
    {
        io.trino.server.protocol.v1.TaskStatus.Builder builder = io.trino.server.protocol.v1.TaskStatus.newBuilder()
                .setTaskId(taskStatus.taskId().toString())
                .setTaskInstanceId(taskStatus.taskInstanceId())
                .setVersion(taskStatus.version())
                .setState(io.trino.server.protocol.v1.TaskState.valueOf(taskStatus.state().name()))
                .setSelf(taskStatus.self().toString())
                .setNodeId(taskStatus.nodeId())
                .setSpeculative(taskStatus.speculative())
                .setQueuedPartitionedDrivers(taskStatus.queuedPartitionedDrivers())
                .setRunningPartitionedDrivers(taskStatus.runningPartitionedDrivers())
                .setOutputDataSizeBytes(taskStatus.outputDataSize().toBytes())
                .setWriterInputDataSizeBytes(taskStatus.writerInputDataSize().toBytes())
                .setPhysicalWrittenDataSizeBytes(taskStatus.physicalWrittenDataSize().toBytes())
                .setMemoryReservationBytes(taskStatus.memoryReservation().toBytes())
                .setPeakMemoryReservationBytes(taskStatus.peakMemoryReservation().toBytes())
                .setRevocableMemoryReservationBytes(taskStatus.revocableMemoryReservation().toBytes())
                .setFullGcCount(taskStatus.fullGcCount())
                .setFullGcTimeNanos(Math.round(taskStatus.fullGcTime().getValue(NANOSECONDS)))
                .setDynamicFiltersVersion(taskStatus.dynamicFiltersVersion())
                .setQueuedPartitionedSplitsWeight(taskStatus.queuedPartitionedSplitsWeight())
                .setRunningPartitionedSplitsWeight(taskStatus.runningPartitionedSplitsWeight());
        taskStatus.failures().forEach(failure -> builder.addFailures(ExecutionFailureInfoProtoMapper.toProto(failure)));
        taskStatus.maxWriterCount().ifPresent(builder::setMaxWriterCount);

        io.trino.server.protocol.v1.OutputBufferStatus.Builder outputBufferStatus = io.trino.server.protocol.v1.OutputBufferStatus.newBuilder()
                .setOverutilized(taskStatus.outputBufferStatus().overutilized())
                .setExchangeSinkInstanceHandleUpdateRequired(taskStatus.outputBufferStatus().exchangeSinkInstanceHandleUpdateRequired());
        taskStatus.outputBufferStatus().outputBuffersVersion().ifPresent(outputBufferStatus::setOutputBuffersVersion);
        builder.setOutputBufferStatus(outputBufferStatus.build());
        return builder.build();
    }

    public static TaskStatus fromProto(io.trino.server.protocol.v1.TaskStatus proto)
    {
        io.trino.server.protocol.v1.OutputBufferStatus outputBufferStatus = proto.getOutputBufferStatus();
        return new TaskStatus(
                TaskId.valueOf(proto.getTaskId()),
                proto.getTaskInstanceId(),
                proto.getVersion(),
                taskState(proto),
                URI.create(proto.getSelf()),
                proto.getNodeId(),
                proto.getSpeculative(),
                proto.getFailuresList().stream()
                        .map(ExecutionFailureInfoProtoMapper::fromProto)
                        .collect(toImmutableList()),
                proto.getQueuedPartitionedDrivers(),
                proto.getRunningPartitionedDrivers(),
                new OutputBufferStatus(
                        outputBufferStatus.hasOutputBuffersVersion() ? OptionalLong.of(outputBufferStatus.getOutputBuffersVersion()) : OptionalLong.empty(),
                        outputBufferStatus.getOverutilized(),
                        outputBufferStatus.getExchangeSinkInstanceHandleUpdateRequired()),
                DataSize.ofBytes(proto.getOutputDataSizeBytes()),
                DataSize.ofBytes(proto.getWriterInputDataSizeBytes()),
                DataSize.ofBytes(proto.getPhysicalWrittenDataSizeBytes()),
                proto.hasMaxWriterCount() ? OptionalInt.of(proto.getMaxWriterCount()) : OptionalInt.empty(),
                DataSize.ofBytes(proto.getMemoryReservationBytes()),
                DataSize.ofBytes(proto.getPeakMemoryReservationBytes()),
                DataSize.ofBytes(proto.getRevocableMemoryReservationBytes()),
                proto.getFullGcCount(),
                new Duration(proto.getFullGcTimeNanos(), NANOSECONDS),
                proto.getDynamicFiltersVersion(),
                proto.getQueuedPartitionedSplitsWeight(),
                proto.getRunningPartitionedSplitsWeight());
    }

    private static TaskState taskState(io.trino.server.protocol.v1.TaskStatus proto)
    {
        io.trino.server.protocol.v1.TaskState state = proto.getState();
        // UNRECOGNIZED has no number, so report the raw wire value
        if (state == io.trino.server.protocol.v1.TaskState.TASK_STATE_UNSPECIFIED || state == io.trino.server.protocol.v1.TaskState.UNRECOGNIZED) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid task state in protobuf TaskStatus: " + proto.getStateValue());
        }
        return TaskState.valueOf(state.name());
    }

    public static byte[] toByteArray(TaskStatus taskStatus)
    {
        return toProto(taskStatus).toByteArray();
    }

    public static TaskStatus parseFrom(byte[] bytes)
    {
        try {
            return fromProto(io.trino.server.protocol.v1.TaskStatus.parser().parseFrom(newCodedInputStream(bytes)));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf TaskStatus", e);
        }
    }
}
