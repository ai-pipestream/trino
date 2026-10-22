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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTpchHandleCodecProvider
{
    private static final Type VARCHAR_25 = createVarcharType(25);
    private static final Type DECIMAL_15_2 = createDecimalType(15, 2);
    private static final Map<String, Type> TYPES = ImmutableMap.of(
            BIGINT.getTypeId().getId(), BIGINT,
            DOUBLE.getTypeId().getId(), DOUBLE,
            VARCHAR_25.getTypeId().getId(), VARCHAR_25,
            DECIMAL_15_2.getTypeId().getId(), DECIMAL_15_2);

    private final ConnectorHandleCodecProvider provider = new TpchHandleCodecProvider(testTypeManager());

    @Test
    public void testTableHandleRoundTrip()
    {
        TpchColumnHandle orderKey = new TpchColumnHandle("orderkey", BIGINT);
        TpchColumnHandle status = new TpchColumnHandle("orderstatus", VARCHAR_25);
        TpchColumnHandle totalPrice = new TpchColumnHandle("totalprice", DECIMAL_15_2);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(
                orderKey, Domain.singleValue(BIGINT, 42L),
                status, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR_25, utf8Slice("F"))), true),
                totalPrice, Domain.singleValue(DECIMAL_15_2, 123456L)));
        TpchTableHandle original = new TpchTableHandle("sf1", "orders", 1.0, constraint);

        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testTableHandleRoundTripRangesAndNulls()
    {
        TpchColumnHandle orderKey = new TpchColumnHandle("orderkey", BIGINT);
        TpchColumnHandle discount = new TpchColumnHandle("discount", DOUBLE);
        TpchColumnHandle comment = new TpchColumnHandle("comment", VARCHAR_25);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(
                orderKey, Domain.create(ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 10L),
                        Range.range(BIGINT, 20L, true, 30L, false),
                        Range.greaterThanOrEqual(BIGINT, 100L)), true),
                discount, Domain.create(ValueSet.ofRanges(Range.all(DOUBLE)), false),
                comment, Domain.onlyNull(VARCHAR_25)));
        TpchTableHandle original = new TpchTableHandle("sf1", "orders", 1.0, constraint);

        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testTableHandleRoundTripNoneConstraint()
    {
        TpchTableHandle original = new TpchTableHandle("sf1", "orders", 1.0, TupleDomain.none());
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testNonScalarDomainValueIsRejected()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(blockBuilder, 1);
        TpchTableHandle handle = new TpchTableHandle(
                "sf1",
                "orders",
                1.0,
                TupleDomain.withColumnDomains(ImmutableMap.of(new TpchColumnHandle("c", arrayType), Domain.singleValue(arrayType, blockBuilder.build()))));

        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThatThrownBy(() -> codec.serialize(handle))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("non-scalar");
    }

    @Test
    public void testCorruptBytesFailWithCategorizedError()
    {
        byte[] truncated = {0x0A, 0x05, 0x01};
        assertThatThrownBy(() -> provider.getTableHandleCodec().orElseThrow().deserialize(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
        assertThatThrownBy(() -> provider.getSplitCodec().orElseThrow().deserialize(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
    }

    @Test
    public void testTableHandleRoundTripUnconstrained()
    {
        TpchTableHandle original = new TpchTableHandle("tiny", "nation", 0.01);
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testColumnHandleRoundTrip()
    {
        TpchColumnHandle original = new TpchColumnHandle("name", VARCHAR_25);
        ConnectorHandleCodec<ColumnHandle> codec = provider.getColumnHandleCodec().orElseThrow();
        TpchColumnHandle decoded = (TpchColumnHandle) codec.deserialize(codec.serialize(original));
        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.type()).isEqualTo(original.type());
    }

    @Test
    public void testSplitRoundTrip()
    {
        TpchSplit original = new TpchSplit(2, 10, ImmutableList.of(HostAddress.fromString("worker1:8080"), HostAddress.fromString("worker2:8080")));
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        TpchSplit decoded = (TpchSplit) codec.deserialize(codec.serialize(original));
        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getAddresses()).isEqualTo(original.getAddresses());
    }

    @Test
    public void testPartitioningHandleRoundTrip()
    {
        TpchPartitioningHandle original = new TpchPartitioningHandle("orders", 1_500_000);
        ConnectorHandleCodec<ConnectorPartitioningHandle> codec = provider.getPartitioningHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testTransactionHandleRoundTrip()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(TpchTransactionHandle.INSTANCE))).isSameAs(TpchTransactionHandle.INSTANCE);
    }

    @Test
    public void testUnsupportedFamiliesAreAbsent()
    {
        assertThat(provider.getOutputTableHandleCodec()).isEmpty();
        assertThat(provider.getInsertTableHandleCodec()).isEmpty();
        assertThat(provider.getIndexHandleCodec()).isEmpty();
        assertThat(provider.getMergeTableHandleCodec()).isEmpty();
        assertThat(provider.getTableExecuteHandleCodec()).isEmpty();
        assertThat(provider.getTableFunctionHandleCodec()).isEmpty();
        assertThat(provider.getTableCredentialsCodec()).isEmpty();
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
