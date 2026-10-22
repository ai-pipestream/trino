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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.memory.MemoryInsertTableHandle.InsertMode;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryHandleCodecProvider
{
    private static final Type VARCHAR = createUnboundedVarcharType();
    private static final Map<String, Type> TYPES = ImmutableMap.of(
            BIGINT.getTypeId().getId(), BIGINT,
            VARCHAR.getTypeId().getId(), VARCHAR);

    private final ConnectorHandleCodecProvider provider = new MemoryHandleCodecProvider(testTypeManager());

    @Test
    public void testTableHandleRoundTrip()
    {
        MemoryTableHandle original = new MemoryTableHandle(7, new SchemaTableName("default", "t"), OptionalLong.of(100), OptionalDouble.of(0.5));
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testTableHandleRoundTripEmptyOptionals()
    {
        MemoryTableHandle original = new MemoryTableHandle(1, new SchemaTableName("default", "t"), OptionalLong.empty(), OptionalDouble.empty());
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testColumnHandleRoundTrip()
    {
        MemoryColumnHandle original = new MemoryColumnHandle(3, "name", VARCHAR);
        ConnectorHandleCodec<ColumnHandle> codec = provider.getColumnHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testSplitRoundTrip()
    {
        MemorySplit original = new MemorySplit(5, 1, 4, HostAddress.fromString("worker:8080"), 1000, OptionalLong.of(50));
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testOutputTableHandleRoundTrip()
    {
        MemoryOutputTableHandle original = new MemoryOutputTableHandle(9, ImmutableSet.of(9L, 10L));
        ConnectorHandleCodec<ConnectorOutputTableHandle> codec = provider.getOutputTableHandleCodec().orElseThrow();
        MemoryOutputTableHandle decoded = (MemoryOutputTableHandle) codec.deserialize(codec.serialize(original));
        assertThat(decoded.table()).isEqualTo(original.table());
        assertThat(decoded.activeTableIds()).isEqualTo(original.activeTableIds());
    }

    @Test
    public void testInsertTableHandleRoundTrip()
    {
        MemoryInsertTableHandle original = new MemoryInsertTableHandle(9, InsertMode.OVERWRITE, ImmutableSet.of(9L));
        ConnectorHandleCodec<ConnectorInsertTableHandle> codec = provider.getInsertTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testInsertTableHandleRoundTripEveryMode()
    {
        ConnectorHandleCodec<ConnectorInsertTableHandle> codec = provider.getInsertTableHandleCodec().orElseThrow();
        for (InsertMode mode : InsertMode.values()) {
            MemoryInsertTableHandle original = new MemoryInsertTableHandle(3, mode, ImmutableSet.of());
            assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
        }
    }

    @Test
    public void testInsertTableHandleUnspecifiedModeIsRejected()
    {
        assertInvalidInsertModeIsRejected(0);
    }

    @Test
    public void testInsertTableHandleUnrecognizedModeIsRejected()
    {
        // A value outside the enum decodes to UNRECOGNIZED, whose getNumber() throws
        assertInvalidInsertModeIsRejected(99);
    }

    @Test
    public void testSplitRoundTripWithoutLimit()
    {
        MemorySplit original = new MemorySplit(5, 0, 1, HostAddress.fromString("worker:8080"), 0, OptionalLong.empty());
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testCorruptBytesFailWithCategorizedError()
    {
        byte[] truncated = {0x12, 0x05, 0x01};
        assertTrinoExceptionThrownBy(() -> provider.getTableHandleCodec().orElseThrow().deserialize(truncated))
                .hasErrorCode(GENERIC_INTERNAL_ERROR);
    }

    private void assertInvalidInsertModeIsRejected(int mode)
    {
        byte[] invalidMode = io.trino.plugin.memory.protobuf.MemoryInsertTableHandle.newBuilder()
                .setTable(3)
                .setModeValue(mode)
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getInsertTableHandleCodec().orElseThrow().deserialize(invalidMode))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessage("Invalid insert mode in protobuf memory insert table handle: " + mode);
    }

    @Test
    public void testTransactionHandleRoundTrip()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(MemoryTransactionHandle.INSTANCE))).isSameAs(MemoryTransactionHandle.INSTANCE);
    }

    private static TypeManager testTypeManager()
    {
        return new TypeManager()
        {
            @Override
            public Type getType(TypeDescriptor descriptor)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Type fromSqlType(String type)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Type getType(TypeId id)
            {
                Type type = TYPES.get(id.getId());
                if (type == null) {
                    throw new IllegalArgumentException("Unknown type id: " + id);
                }
                return type;
            }

            @Override
            public boolean isTypeRegistered(String name)
            {
                return false;
            }

            @Override
            public TypeOperators getTypeOperators()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
