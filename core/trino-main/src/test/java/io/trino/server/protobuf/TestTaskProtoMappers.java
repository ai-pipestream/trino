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
import com.google.protobuf.ByteString;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.ErrorLocation;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.server.FailTaskRequest;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.HostAddress;
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
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTER_DOMAINS;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.ColorType.COLOR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class TestTaskProtoMappers
{
    private final DynamicFilterDomainsProtoMapper dynamicFilterDomainsProtoMapper = new DynamicFilterDomainsProtoMapper(
            new DomainProtoCodec(TESTING_TYPE_MANAGER, TESTING_BLOCK_ENCODING_SERDE));

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
    public void testTaskStatusRoundTripEveryTaskState()
    {
        TaskStatus initial = TaskStatus.initialTaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.0.0.0"),
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0"),
                "worker-0",
                false);
        for (TaskState state : TaskState.values()) {
            TaskStatus original = new TaskStatus(
                    initial.taskId(),
                    initial.taskInstanceId(),
                    initial.version(),
                    state,
                    initial.self(),
                    initial.nodeId(),
                    initial.speculative(),
                    initial.failures(),
                    initial.queuedPartitionedDrivers(),
                    initial.runningPartitionedDrivers(),
                    initial.outputBufferStatus(),
                    initial.outputDataSize(),
                    initial.writerInputDataSize(),
                    initial.physicalWrittenDataSize(),
                    initial.maxWriterCount(),
                    initial.memoryReservation(),
                    initial.peakMemoryReservation(),
                    initial.revocableMemoryReservation(),
                    initial.fullGcCount(),
                    initial.fullGcTime(),
                    initial.dynamicFiltersVersion(),
                    initial.queuedPartitionedSplitsWeight(),
                    initial.runningPartitionedSplitsWeight());
            assertThat(TaskStatusProtoMapper.parseFrom(TaskStatusProtoMapper.toByteArray(original)).state()).isEqualTo(state);
        }
    }

    @Test
    public void testFailTaskRequestRoundTripEveryErrorType()
    {
        for (ErrorType errorType : ErrorType.values()) {
            ExecutionFailureInfo failure = new ExecutionFailureInfo(
                    "io.trino.spi.TrinoException",
                    "failed",
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    null,
                    new ErrorCode(1, "TEST_ERROR", errorType, false),
                    null);
            FailTaskRequest decoded = FailTaskRequestProtoMapper.parseFrom(FailTaskRequestProtoMapper.toByteArray(new FailTaskRequest(failure)));
            assertThat(decoded.getFailureInfo().errorCode().getType()).isEqualTo(errorType);
        }
    }

    @Test
    public void testFailTaskRequestRoundTrip()
    {
        FailTaskRequest original = new FailTaskRequest(failureInfo());
        FailTaskRequest decoded = FailTaskRequestProtoMapper.parseFrom(FailTaskRequestProtoMapper.toByteArray(original));
        assertFailureInfoEquals(decoded.getFailureInfo(), original.getFailureInfo());
    }

    @Test
    public void testFailTaskRequestRoundTripDeepCauseChain()
    {
        // Failures.toFailure does not bound the cause chain, and the JSON codec
        // accepts a nesting depth well beyond protobuf's default limit of 100
        ExecutionFailureInfo failure = failureInfo();
        for (int depth = 0; depth < 500; depth++) {
            failure = new ExecutionFailureInfo("java.lang.RuntimeException", "wrapped " + depth, failure, ImmutableList.of(), ImmutableList.of(), null, null, null);
        }
        FailTaskRequest original = new FailTaskRequest(failure);
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
        assertTrinoExceptionThrownBy(() -> TaskStatusProtoMapper.parseFrom(truncated))
                .hasErrorCode(GENERIC_INTERNAL_ERROR);
        assertTrinoExceptionThrownBy(() -> FailTaskRequestProtoMapper.parseFrom(truncated))
                .hasErrorCode(GENERIC_INTERNAL_ERROR);
        assertTrinoExceptionThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(truncated))
                .hasErrorCode(GENERIC_INTERNAL_ERROR);
    }

    @Test
    public void testInitialDynamicFilterDomainsEncodeToEmptyMessage()
    {
        // Every field has its default value, so the encoding is zero bytes
        byte[] bytes = dynamicFilterDomainsProtoMapper.toByteArray(INITIAL_DYNAMIC_FILTER_DOMAINS);
        assertThat(bytes).isEmpty();
        VersionedDynamicFilterDomains decoded = dynamicFilterDomainsProtoMapper.parseFrom(bytes);
        assertThat(decoded.getVersion()).isEqualTo(INITIAL_DYNAMIC_FILTER_DOMAINS.getVersion());
        assertThat(decoded.getDynamicFilterDomains()).isEmpty();
    }

    @Test
    public void testMismatchedRangeBoundsAreRejected()
    {
        io.trino.protobuf.handles.v1.Domain domain = dynamicFilterDomainsProtoMapper.toProto(new VersionedDynamicFilterDomains(
                        1,
                        ImmutableMap.of(new DynamicFilterId("df"), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 5L, true)), false))))
                .getDynamicFilterDomainsOrThrow("df");
        // Two bounds in the block but three inclusive flags
        byte[] mismatched = io.trino.server.protocol.v1.VersionedDynamicFilterDomains.newBuilder()
                .setVersion(1)
                .putDynamicFilterDomains("df", domain.toBuilder()
                        .setSortedRanges(domain.getSortedRanges().toBuilder().addInclusive(true))
                        .build())
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(mismatched))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("2 bounds and 3 inclusive flags");
    }

    @Test
    public void testInvertedRangeIsRejected()
    {
        io.trino.protobuf.handles.v1.Domain low = dynamicFilterDomainsProtoMapper.toProto(new VersionedDynamicFilterDomains(
                        1,
                        ImmutableMap.of(new DynamicFilterId("df"), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 5L, true, 1L + 5L, true)), false))))
                .getDynamicFilterDomainsOrThrow("df");
        io.trino.protobuf.handles.v1.Domain high = dynamicFilterDomainsProtoMapper.toProto(new VersionedDynamicFilterDomains(
                        1,
                        ImmutableMap.of(new DynamicFilterId("df"), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))))
                .getDynamicFilterDomainsOrThrow("df");
        // Splice the bounds block of a [1, 2] range under the flags of another domain,
        // then swap to a block whose low bound is above its high bound
        byte[] inverted = io.trino.server.protocol.v1.VersionedDynamicFilterDomains.newBuilder()
                .setVersion(1)
                .putDynamicFilterDomains("df", low.toBuilder()
                        .setSortedRanges(low.getSortedRanges().toBuilder().setBounds(invertedBounds(high)))
                        .build())
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(inverted))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid");
    }

    private static ByteString invertedBounds(io.trino.protobuf.handles.v1.Domain domain)
    {
        // A bigint block with values [2, 1]: the encoding of the sorted block [1, 2]
        // with its two 8-byte little-endian values swapped
        byte[] bytes = domain.getSortedRanges().getBounds().toByteArray();
        int valuesStart = bytes.length - 16;
        byte[] swapped = bytes.clone();
        System.arraycopy(bytes, valuesStart + 8, swapped, valuesStart, 8);
        System.arraycopy(bytes, valuesStart, swapped, valuesStart + 8, 8);
        return ByteString.copyFrom(swapped);
    }

    @Test
    public void testUnspecifiedEnumValuesAreRejected()
    {
        assertInvalidEnumValuesAreRejected(0);
    }

    @Test
    public void testUnrecognizedEnumValuesAreRejected()
    {
        // A value outside the enum decodes to UNRECOGNIZED, whose getNumber() throws
        assertInvalidEnumValuesAreRejected(99);
    }

    @Test
    public void testDomainWithoutValuesIsRejected()
    {
        byte[] domainWithoutValues = io.trino.server.protocol.v1.VersionedDynamicFilterDomains.newBuilder()
                .setVersion(1)
                .putDynamicFilterDomains("df", io.trino.protobuf.handles.v1.Domain.newBuilder()
                        .setTypeId(BIGINT.getTypeId().getId())
                        .build())
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> dynamicFilterDomainsProtoMapper.parseFrom(domainWithoutValues))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no value set");
    }

    private static void assertInvalidEnumValuesAreRejected(int value)
    {
        TaskStatus taskStatus = TaskStatus.initialTaskStatus(
                TaskId.valueOf("20240101_000000_00000_xyz.0.0.0"),
                URI.create("http://worker.example.com:8080/v1/task/20240101_000000_00000_xyz.0.0.0"),
                "worker-0",
                false);
        byte[] invalidState = TaskStatusProtoMapper.toProto(taskStatus).toBuilder()
                .setStateValue(value)
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> TaskStatusProtoMapper.parseFrom(invalidState))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessage("Invalid task state in protobuf TaskStatus: " + value);

        io.trino.server.protocol.v1.FailTaskRequest failTaskRequest = FailTaskRequestProtoMapper.toProto(new FailTaskRequest(failureInfo()));
        byte[] invalidErrorType = failTaskRequest.toBuilder()
                .setFailureInfo(failTaskRequest.getFailureInfo().toBuilder()
                        .setErrorCode(failTaskRequest.getFailureInfo().getErrorCode().toBuilder()
                                .setTypeValue(value)))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> FailTaskRequestProtoMapper.parseFrom(invalidErrorType))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessage("Invalid error type in protobuf ExecutionFailureInfo: " + value);
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
