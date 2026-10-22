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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
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
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf handle codecs for the families the memory connector implements:
 * table, column, split, output, insert and transaction handles.
 */
public class MemoryHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final TypeManager typeManager;

    public MemoryHandleCodecProvider(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    MemoryTableHandle tableHandle = (MemoryTableHandle) handle;
                    io.trino.plugin.memory.protobuf.MemoryTableHandle.Builder builder = io.trino.plugin.memory.protobuf.MemoryTableHandle.newBuilder()
                            .setId(tableHandle.id())
                            .setSchemaName(tableHandle.name().getSchemaName())
                            .setTableName(tableHandle.name().getTableName());
                    tableHandle.limit().ifPresent(builder::setLimit);
                    tableHandle.sampleRatio().ifPresent(builder::setSampleRatio);
                    return builder.build();
                },
                proto -> new MemoryTableHandle(
                        proto.getId(),
                        new SchemaTableName(proto.getSchemaName(), proto.getTableName()),
                        proto.hasLimit() ? OptionalLong.of(proto.getLimit()) : OptionalLong.empty(),
                        proto.hasSampleRatio() ? OptionalDouble.of(proto.getSampleRatio()) : OptionalDouble.empty()),
                io.trino.plugin.memory.protobuf.MemoryTableHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    MemoryColumnHandle columnHandle = (MemoryColumnHandle) handle;
                    return io.trino.plugin.memory.protobuf.MemoryColumnHandle.newBuilder()
                            .setColumnIndex(columnHandle.columnIndex())
                            .setName(columnHandle.name())
                            .setTypeId(columnHandle.type().getTypeId().getId())
                            .build();
                },
                proto -> new MemoryColumnHandle(proto.getColumnIndex(), proto.getName(), typeManager.getType(TypeId.of(proto.getTypeId()))),
                io.trino.plugin.memory.protobuf.MemoryColumnHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(codec(
                handle -> {
                    MemorySplit split = (MemorySplit) handle;
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
                        proto.hasLimit() ? OptionalLong.of(proto.getLimit()) : OptionalLong.empty()),
                io.trino.plugin.memory.protobuf.MemorySplit.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    MemoryOutputTableHandle outputTableHandle = (MemoryOutputTableHandle) handle;
                    return io.trino.plugin.memory.protobuf.MemoryOutputTableHandle.newBuilder()
                            .setTable(outputTableHandle.table())
                            .addAllActiveTableIds(outputTableHandle.activeTableIds())
                            .build();
                },
                proto -> new MemoryOutputTableHandle(proto.getTable(), proto.getActiveTableIdsList().stream().collect(toImmutableSet())),
                io.trino.plugin.memory.protobuf.MemoryOutputTableHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    MemoryInsertTableHandle insertTableHandle = (MemoryInsertTableHandle) handle;
                    return io.trino.plugin.memory.protobuf.MemoryInsertTableHandle.newBuilder()
                            .setTable(insertTableHandle.table())
                            .setMode(InsertMode.valueOf(insertTableHandle.mode().name()))
                            .addAllActiveTableIds(insertTableHandle.activeTableIds())
                            .build();
                },
                proto -> new MemoryInsertTableHandle(
                        proto.getTable(),
                        insertMode(proto.getMode()),
                        proto.getActiveTableIdsList().stream().collect(toImmutableSet())),
                io.trino.plugin.memory.protobuf.MemoryInsertTableHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(codec(
                _ -> io.trino.plugin.memory.protobuf.MemoryTransactionHandle.getDefaultInstance(),
                _ -> MemoryTransactionHandle.INSTANCE,
                io.trino.plugin.memory.protobuf.MemoryTransactionHandle.parser()));
    }

    private static MemoryInsertTableHandle.InsertMode insertMode(InsertMode mode)
    {
        if (mode == InsertMode.INSERT_MODE_UNSPECIFIED || mode == InsertMode.UNRECOGNIZED) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid insert mode in protobuf memory insert table handle: " + mode.getNumber());
        }
        return MemoryInsertTableHandle.InsertMode.valueOf(mode.name());
    }

    private static <H, P extends Message> ConnectorHandleCodec<H> codec(Function<H, P> toProto, Function<P, H> fromProto, Parser<P> parser)
    {
        return new ConnectorHandleCodec<>()
        {
            @Override
            public byte[] serialize(H handle)
            {
                return toProto.apply(handle).toByteArray();
            }

            @Override
            public H deserialize(byte[] bytes)
            {
                try {
                    return fromProto.apply(parser.parseFrom(bytes));
                }
                catch (InvalidProtocolBufferException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf memory handle", e);
                }
            }
        };
    }
}
