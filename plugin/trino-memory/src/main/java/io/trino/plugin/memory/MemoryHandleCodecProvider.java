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
package io.trino.plugin.memory;

import io.trino.plugin.memory.protobuf.InsertMode;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf handle codecs for the families the memory connector implements:
 * table, column, split, output, insert and transaction handles.
 */
public class MemoryHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final ConnectorHandleCodec<ConnectorTableHandle> tableHandleCodec;
    private final ConnectorHandleCodec<ColumnHandle> columnHandleCodec;
    private final ConnectorHandleCodec<ConnectorSplit> splitCodec;
    private final ConnectorHandleCodec<ConnectorOutputTableHandle> outputTableHandleCodec;
    private final ConnectorHandleCodec<ConnectorInsertTableHandle> insertTableHandleCodec;
    private final ConnectorHandleCodec<ConnectorTransactionHandle> transactionHandleCodec;

    public MemoryHandleCodecProvider(TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");

        tableHandleCodec = protobufHandleCodec(
                MemoryTableHandle.class,
                io.trino.plugin.memory.protobuf.MemoryTableHandle.parser(),
                handle -> {
                    io.trino.plugin.memory.protobuf.MemoryTableHandle.Builder builder = io.trino.plugin.memory.protobuf.MemoryTableHandle.newBuilder()
                            .setId(handle.id())
                            .setSchemaName(handle.name().getSchemaName())
                            .setTableName(handle.name().getTableName());
                    handle.limit().ifPresent(builder::setLimit);
                    handle.sampleRatio().ifPresent(builder::setSampleRatio);
                    return builder.build();
                },
                proto -> new MemoryTableHandle(
                        proto.getId(),
                        new SchemaTableName(proto.getSchemaName(), proto.getTableName()),
                        proto.hasLimit() ? OptionalLong.of(proto.getLimit()) : OptionalLong.empty(),
                        proto.hasSampleRatio() ? OptionalDouble.of(proto.getSampleRatio()) : OptionalDouble.empty()));

        columnHandleCodec = protobufHandleCodec(
                MemoryColumnHandle.class,
                io.trino.plugin.memory.protobuf.MemoryColumnHandle.parser(),
                handle -> io.trino.plugin.memory.protobuf.MemoryColumnHandle.newBuilder()
                        .setColumnIndex(handle.columnIndex())
                        .setName(handle.name())
                        .setTypeId(handle.type().getTypeId().getId())
                        .build(),
                proto -> new MemoryColumnHandle(proto.getColumnIndex(), proto.getName(), typeManager.getType(TypeId.of(proto.getTypeId()))));

        splitCodec = protobufHandleCodec(
                MemorySplit.class,
                io.trino.plugin.memory.protobuf.MemorySplit.parser(),
                split -> {
                    io.trino.plugin.memory.protobuf.MemorySplit.Builder builder = io.trino.plugin.memory.protobuf.MemorySplit.newBuilder()
                            .setTable(split.table())
                            .setPartNumber(split.partNumber())
                            .setTotalPartsPerWorker(split.totalPartsPerWorker())
                            .setAddress(split.address().toString())
                            .setExpectedRows(split.expectedRows());
                    split.limit().ifPresent(builder::setLimit);
                    return builder.build();
                },
                proto -> new MemorySplit(
                        proto.getTable(),
                        proto.getPartNumber(),
                        proto.getTotalPartsPerWorker(),
                        HostAddress.fromString(proto.getAddress()),
                        proto.getExpectedRows(),
                        proto.hasLimit() ? OptionalLong.of(proto.getLimit()) : OptionalLong.empty()));

        outputTableHandleCodec = protobufHandleCodec(
                MemoryOutputTableHandle.class,
                io.trino.plugin.memory.protobuf.MemoryOutputTableHandle.parser(),
                handle -> io.trino.plugin.memory.protobuf.MemoryOutputTableHandle.newBuilder()
                        .setTable(handle.table())
                        .addAllActiveTableIds(handle.activeTableIds())
                        .build(),
                proto -> new MemoryOutputTableHandle(proto.getTable(), proto.getActiveTableIdsList().stream().collect(toImmutableSet())));

        insertTableHandleCodec = protobufHandleCodec(
                MemoryInsertTableHandle.class,
                io.trino.plugin.memory.protobuf.MemoryInsertTableHandle.parser(),
                handle -> io.trino.plugin.memory.protobuf.MemoryInsertTableHandle.newBuilder()
                        .setTable(handle.table())
                        .setMode(toProto(handle.mode()))
                        .addAllActiveTableIds(handle.activeTableIds())
                        .build(),
                proto -> new MemoryInsertTableHandle(
                        proto.getTable(),
                        insertMode(proto),
                        proto.getActiveTableIdsList().stream().collect(toImmutableSet())));

        transactionHandleCodec = protobufHandleCodec(
                MemoryTransactionHandle.class,
                io.trino.plugin.memory.protobuf.MemoryTransactionHandle.parser(),
                _ -> io.trino.plugin.memory.protobuf.MemoryTransactionHandle.getDefaultInstance(),
                _ -> MemoryTransactionHandle.INSTANCE);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.of(tableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(columnHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(splitCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec()
    {
        return Optional.of(outputTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec()
    {
        return Optional.of(insertTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(transactionHandleCodec);
    }

    private static InsertMode toProto(MemoryInsertTableHandle.InsertMode mode)
    {
        return switch (mode) {
            case APPEND -> InsertMode.APPEND;
            case OVERWRITE -> InsertMode.OVERWRITE;
        };
    }

    private static MemoryInsertTableHandle.InsertMode insertMode(io.trino.plugin.memory.protobuf.MemoryInsertTableHandle proto)
    {
        return switch (proto.getMode()) {
            case APPEND -> MemoryInsertTableHandle.InsertMode.APPEND;
            case OVERWRITE -> MemoryInsertTableHandle.InsertMode.OVERWRITE;
            // UNRECOGNIZED has no number, so report the raw wire value
            case INSERT_MODE_UNSPECIFIED, UNRECOGNIZED -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid insert mode in protobuf memory insert table handle: " + proto.getModeValue());
        };
    }
}
