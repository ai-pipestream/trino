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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.ErrorLocation;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.server.FailTaskRequest;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.ColorType.COLOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

public class TestTaskProtoMappers
{
    private final DynamicFilterDomainsProtoMapper dynamicFilterDomainsProtoMapper = new DynamicFilterDomainsProtoMapper(
            new DomainProtoMapper(TESTING_BLOCK_ENCODING_SERDE, TESTING_TYPE_MANAGER));

    @Test
    public void testTaskStatusRoundTrip()
    {
        TaskStatus original = new TaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.1.2.0"),
                42,
                7,
                FAILED,
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.1.2.0"),
                "worker-1",
                true,
                ImmutableList.of(failureInfo()),
                3,
                5,
                new OutputBufferStatus(OptionalLong.of(9), true, false),
                DataSize.ofBytes(1024),
                DataSize.ofBytes(2048),
                DataSize.ofBytes(4096),
                OptionalInt.of(11),
                DataSize.ofBytes(8192),
                DataSize.ofBytes(16384),
                DataSize.ofBytes(32768),
                2,
                new Duration(123.456789, MILLISECONDS),
                13,
                17,
                19);
        TaskStatus decoded = TaskStatusProtoMapper.parseFrom(TaskStatusProtoMapper.toByteArray(original));
        assertTaskStatusEquals(decoded, original);
    }

    @Test
    public void testTaskStatusRoundTripEmptyOptionals()
    {
        TaskStatus original = TaskStatus.initialTaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.0.0.0"),
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0"),
                "worker-0",
                false);
        TaskStatus decoded = TaskStatusProtoMapper.parseFrom(TaskStatusProtoMapper.toByteArray(original));
        assertTaskStatusEquals(decoded, original);
    }

    @Test
    public void testFailTaskRequestRoundTrip()
    {
        FailTaskRequest original = new FailTaskRequest(failureInfo());
        FailTaskRequest decoded = FailTaskRequestProtoMapper.parseFrom(FailTaskRequestProtoMapper.toByteArray(original));
        assertFailureInfoEquals(decoded.getFailureInfo(), original.getFailureInfo());
    }

    @Test
    public void testDynamicFilterDomainsRoundTrip()
    {
        Type mapType = TESTING_TYPE_MANAGER.fromSqlType("map(bigint, bigint)");
        Type arrayType = new ArrayType(BIGINT);
        VersionedDynamicFilterDomains original = new VersionedDynamicFilterDomains(
                23,
                ImmutableMap.<DynamicFilterId, Domain>builder()
                        .put(new DynamicFilterId("df_all"), Domain.all(BIGINT))
                        .put(new DynamicFilterId("df_none"), Domain.none(BIGINT))
                        .put(new DynamicFilterId("df_only_null"), Domain.onlyNull(VARCHAR))
                        .put(new DynamicFilterId("df_single"), Domain.singleValue(BIGINT, 42L))
                        .put(new DynamicFilterId("df_multiple"), Domain.multipleValues(VARCHAR, List.of(utf8Slice("apple"), utf8Slice("banana"))))
                        .put(new DynamicFilterId("df_range"), Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.5), Range.lessThanOrEqual(DOUBLE, -2.5)), true))
                        .put(new DynamicFilterId("df_discrete"), Domain.create(ValueSet.copyOf(arrayType, List.of(createLongsBlock(1, 2), createLongsBlock(3))), false))
                        .put(new DynamicFilterId("df_all_or_none"), Domain.all(mapType))
                        .buildOrThrow());
        VersionedDynamicFilterDomains decoded = dynamicFilterDomainsProtoMapper.parseFrom(dynamicFilterDomainsProtoMapper.toByteArray(original));
        assertThat(decoded.getVersion()).isEqualTo(original.getVersion());
        assertThat(decoded.getDynamicFilterDomains()).isEqualTo(original.getDynamicFilterDomains());
    }

    @Test
    public void testDynamicFilterDomainsRoundTripEquatableAndUnboundedValues()
    {
        // COLOR is comparable but not orderable, so these domains are backed by EquatableValueSet
        VersionedDynamicFilterDomains original = new VersionedDynamicFilterDomains(
                5,
                ImmutableMap.<DynamicFilterId, Domain>builder()
                        .put(new DynamicFilterId("df_equatable"), Domain.multipleValues(COLOR, List.of(1L, 2L, 3L)))
                        .put(new DynamicFilterId("df_equatable_complement"), Domain.create(ValueSet.of(COLOR, 4L).complement(), true))
                        .put(new DynamicFilterId("df_equatable_none"), Domain.create(ValueSet.none(COLOR), false))
                        .put(new DynamicFilterId("df_range_all"), Domain.create(ValueSet.ofRanges(Range.all(BIGINT)), false))
                        .put(new DynamicFilterId("df_range_unbounded_low"), Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 7L)), false))
                        .put(new DynamicFilterId("df_not_null"), Domain.notNull(VARCHAR))
                        .put(new DynamicFilterId("df_large"), Domain.multipleValues(BIGINT, LongStream.range(0, 10_000).boxed().toList()))
                        .buildOrThrow());
        VersionedDynamicFilterDomains decoded = dynamicFilterDomainsProtoMapper.parseFrom(dynamicFilterDomainsProtoMapper.toByteArray(original));
        assertThat(decoded.getVersion()).isEqualTo(original.getVersion());
        assertThat(decoded.getDynamicFilterDomains()).isEqualTo(original.getDynamicFilterDomains());
    }

    @Test
    public void testCorruptBytesFailWithCategorizedError()
    {
        // field 1, length-delimited, declared length 5 but only one byte follows
        byte[] truncated = {0x0A, 0x05, 0x01};
        assertThatThrownBy(() -> TaskStatusProtoMapper.parseFrom(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
        assertThatThrownBy(() -> FailTaskRequestProtoMapper.parseFrom(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
        assertThatThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
    }

    @Test
    public void testUnspecifiedEnumValuesAreRejected()
    {
        TaskStatus taskStatus = TaskStatus.initialTaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.0.0.0"),
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0"),
                "worker-0",
                false);
        byte[] unspecifiedState = TaskStatusProtoMapper.toProto(taskStatus).toBuilder()
                .setState(io.trino.server.protocol.v1.TaskState.TASK_STATE_UNSPECIFIED)
                .build()
                .toByteArray();
        assertThatThrownBy(() -> TaskStatusProtoMapper.parseFrom(unspecifiedState))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid task state");

        io.trino.server.protocol.v1.FailTaskRequest failTaskRequest = FailTaskRequestProtoMapper.toProto(new FailTaskRequest(failureInfo()));
        byte[] unspecifiedErrorType = failTaskRequest.toBuilder()
                .setFailureInfo(failTaskRequest.getFailureInfo().toBuilder()
                        .setErrorCode(failTaskRequest.getFailureInfo().getErrorCode().toBuilder()
                                .setType(io.trino.server.protocol.v1.ErrorType.ERROR_TYPE_UNSPECIFIED)))
                .build()
                .toByteArray();
        assertThatThrownBy(() -> FailTaskRequestProtoMapper.parseFrom(unspecifiedErrorType))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid error type");

        byte[] domainWithoutValues = io.trino.server.protocol.v1.VersionedDynamicFilterDomains.newBuilder()
                .setVersion(1)
                .putDynamicFilterDomains("df", io.trino.server.protocol.v1.Domain.newBuilder()
                        .setTypeId(BIGINT.getTypeId().getId())
                        .build())
                .build()
                .toByteArray();
        assertThatThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(domainWithoutValues))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("no values set");
    }

    private static void assertTaskStatusEquals(TaskStatus actual, TaskStatus expected)
    {
        assertThat(actual.taskId()).isEqualTo(expected.taskId());
        assertThat(actual.taskInstanceId()).isEqualTo(expected.taskInstanceId());
        assertThat(actual.version()).isEqualTo(expected.version());
        assertThat(actual.state()).isEqualTo(expected.state());
        assertThat(actual.self()).isEqualTo(expected.self());
        assertThat(actual.nodeId()).isEqualTo(expected.nodeId());
        assertThat(actual.speculative()).isEqualTo(expected.speculative());
        assertThat(actual.failures()).hasSize(expected.failures().size());
        for (int i = 0; i < expected.failures().size(); i++) {
            assertFailureInfoEquals(actual.failures().get(i), expected.failures().get(i));
        }
        assertThat(actual.queuedPartitionedDrivers()).isEqualTo(expected.queuedPartitionedDrivers());
        assertThat(actual.runningPartitionedDrivers()).isEqualTo(expected.runningPartitionedDrivers());
        assertThat(actual.outputBufferStatus()).isEqualTo(expected.outputBufferStatus());
        assertThat(actual.outputDataSize()).isEqualTo(expected.outputDataSize());
        assertThat(actual.writerInputDataSize()).isEqualTo(expected.writerInputDataSize());
        assertThat(actual.physicalWrittenDataSize()).isEqualTo(expected.physicalWrittenDataSize());
        assertThat(actual.maxWriterCount()).isEqualTo(expected.maxWriterCount());
        assertThat(actual.memoryReservation()).isEqualTo(expected.memoryReservation());
        assertThat(actual.peakMemoryReservation()).isEqualTo(expected.peakMemoryReservation());
        assertThat(actual.revocableMemoryReservation()).isEqualTo(expected.revocableMemoryReservation());
        assertThat(actual.fullGcCount()).isEqualTo(expected.fullGcCount());
        assertThat(actual.fullGcTime().getValue(NANOSECONDS)).isCloseTo(expected.fullGcTime().getValue(NANOSECONDS), within(1.0));
        assertThat(actual.dynamicFiltersVersion()).isEqualTo(expected.dynamicFiltersVersion());
        assertThat(actual.queuedPartitionedSplitsWeight()).isEqualTo(expected.queuedPartitionedSplitsWeight());
        assertThat(actual.runningPartitionedSplitsWeight()).isEqualTo(expected.runningPartitionedSplitsWeight());
    }

    private static void assertFailureInfoEquals(ExecutionFailureInfo actual, ExecutionFailureInfo expected)
    {
        assertThat(actual.type()).isEqualTo(expected.type());
        assertThat(actual.message()).isEqualTo(expected.message());
        if (expected.cause() == null) {
            assertThat(actual.cause()).isNull();
        }
        else {
            assertFailureInfoEquals(actual.cause(), expected.cause());
        }
        assertThat(actual.suppressed()).hasSize(expected.suppressed().size());
        for (int i = 0; i < expected.suppressed().size(); i++) {
            assertFailureInfoEquals(actual.suppressed().get(i), expected.suppressed().get(i));
        }
        assertThat(actual.stack()).isEqualTo(expected.stack());
        if (expected.errorLocation() == null) {
            assertThat(actual.errorLocation()).isNull();
        }
        else {
            assertThat(actual.errorLocation().getLineNumber()).isEqualTo(expected.errorLocation().getLineNumber());
            assertThat(actual.errorLocation().getColumnNumber()).isEqualTo(expected.errorLocation().getColumnNumber());
        }
        if (expected.errorCode() == null) {
            assertThat(actual.errorCode()).isNull();
        }
        else {
            assertThat(actual.errorCode().getCode()).isEqualTo(expected.errorCode().getCode());
            assertThat(actual.errorCode().getName()).isEqualTo(expected.errorCode().getName());
            assertThat(actual.errorCode().getType()).isEqualTo(expected.errorCode().getType());
            assertThat(actual.errorCode().isFatal()).isEqualTo(expected.errorCode().isFatal());
        }
        assertThat(actual.remoteHost()).isEqualTo(expected.remoteHost());
    }

    private static ExecutionFailureInfo failureInfo()
    {
        ExecutionFailureInfo cause = new ExecutionFailureInfo(
                "java.lang.IllegalStateException",
                "bad state",
                null,
                ImmutableList.of(),
                ImmutableList.of("io.trino.Foo.bar(Foo.java:10)"),
                null,
                null,
                null);
        return new ExecutionFailureInfo(
                "io.trino.spi.TrinoException",
                "query failed",
                cause,
                ImmutableList.of(cause),
                ImmutableList.of("io.trino.Bar.baz(Bar.java:20)", "io.trino.Qux.run(Qux.java:30)"),
                new ErrorLocation(3, 15),
                new ErrorCode(65536, "GENERIC_INTERNAL_ERROR", ErrorType.INTERNAL_ERROR, false),
                HostAddress.fromString("worker.example.com:8080"));
    }
}
