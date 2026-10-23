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
package io.trino.connector.system;

import io.trino.connector.v1.GlobalSystemTableFunctionHandle;
import io.trino.operator.table.ExcludeColumnsFunction.ExcludeColumnsFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionSplit;
import io.trino.operator.table.json.JsonTable.JsonTableFunctionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.server.protobuf.ProtoIdentifiers.parseTransactionId;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Protobuf codecs for the handles of {@link GlobalSystemConnector}. The
 * {@code json_table} function handle carries a processing plan of
 * expressions, which travels only once plan fragments have a protobuf form.
 */
public final class GlobalSystemHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private static final ConnectorHandleCodec<ConnectorTransactionHandle> TRANSACTION_HANDLE_CODEC = protobufHandleCodec(
            GlobalSystemTransactionHandle.class,
            io.trino.connector.v1.GlobalSystemTransactionHandle.parser(),
            handle -> io.trino.connector.v1.GlobalSystemTransactionHandle.newBuilder()
                    .setTransactionId(handle.transactionId().toString())
                    .build(),
            proto -> new GlobalSystemTransactionHandle(parseTransactionId(proto.getTransactionId())));

    private static final ConnectorHandleCodec<ConnectorTableFunctionHandle> TABLE_FUNCTION_HANDLE_CODEC = new ConnectorHandleCodec<>()
    {
        private final ConnectorHandleCodec<ConnectorTableFunctionHandle> delegate = protobufHandleCodec(
                ConnectorTableFunctionHandle.class,
                GlobalSystemTableFunctionHandle.parser(),
                GlobalSystemHandleCodecProvider::toProto,
                GlobalSystemHandleCodecProvider::fromProto);

        @Override
        public byte[] serialize(ConnectorTableFunctionHandle handle)
        {
            if (handle instanceof JsonTableFunctionHandle) {
                throw new TrinoException(NOT_SUPPORTED, "The json_table function handle has no protobuf encoding yet; it carries a plan of expressions");
            }
            return delegate.serialize(handle);
        }

        @Override
        public ConnectorTableFunctionHandle deserialize(byte[] bytes)
        {
            return delegate.deserialize(bytes);
        }
    };

    private static final ConnectorHandleCodec<ConnectorSplit> SPLIT_CODEC = protobufHandleCodec(
            SequenceFunctionSplit.class,
            io.trino.connector.v1.SequenceFunctionSplit.parser(),
            split -> io.trino.connector.v1.SequenceFunctionSplit.newBuilder()
                    .setStart(split.getStart())
                    .setStop(split.getStop())
                    .build(),
            proto -> new SequenceFunctionSplit(proto.getStart(), proto.getStop()));

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(TRANSACTION_HANDLE_CODEC);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableFunctionHandle>> getTableFunctionHandleCodec()
    {
        return Optional.of(TABLE_FUNCTION_HANDLE_CODEC);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(SPLIT_CODEC);
    }

    private static GlobalSystemTableFunctionHandle toProto(ConnectorTableFunctionHandle handle)
    {
        GlobalSystemTableFunctionHandle.Builder builder = GlobalSystemTableFunctionHandle.newBuilder();
        switch (handle) {
            case ExcludeColumnsFunctionHandle _ -> builder.setExcludeColumns(io.trino.connector.v1.ExcludeColumnsFunctionHandle.getDefaultInstance());
            case SequenceFunctionHandle sequence -> builder.setSequence(io.trino.connector.v1.SequenceFunctionHandle.newBuilder()
                    .setStart(sequence.start())
                    .setStop(sequence.stop())
                    .setStep(sequence.step()));
            default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown global system table function handle " + handle.getClass().getName());
        }
        return builder.build();
    }

    private static ConnectorTableFunctionHandle fromProto(GlobalSystemTableFunctionHandle proto)
    {
        return switch (proto.getFunctionCase()) {
            case EXCLUDE_COLUMNS -> new ExcludeColumnsFunctionHandle();
            case SEQUENCE -> new SequenceFunctionHandle(proto.getSequence().getStart(), proto.getSequence().getStop(), proto.getSequence().getStep());
            case FUNCTION_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf global system table function handle has no function");
        };
    }
}
