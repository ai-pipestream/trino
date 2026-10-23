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
import io.trino.plugin.tpch.protobuf.TpchColumnDomain;
import io.trino.plugin.tpch.protobuf.TpchTupleDomain;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.protobuf.handles.TestingBlockEncodingSerde;
import io.trino.protobuf.handles.TestingTypeManager;
import io.trino.protobuf.handles.v1.TupleDomainKind;
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
import io.trino.spi.type.TypeManager;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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
    private static final Type BIGINT_ARRAY = new ArrayType(BIGINT);
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager(List.of(VARCHAR_25, DECIMAL_15_2, BIGINT_ARRAY));

    private final ConnectorHandleCodecProvider provider = new TpchHandleCodecProvider(TYPE_MANAGER, new DomainProtoCodec(TYPE_MANAGER, new TestingBlockEncodingSerde(TYPE_MANAGER)));

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
    public void testNonScalarDomainValueRoundTrip()
    {
        // domain values are blocks on the wire, so a value of a structural type needs no special case
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 2);
        BIGINT.writeLong(blockBuilder, 1);
        BIGINT.writeLong(blockBuilder, 2);
        TpchTableHandle original = new TpchTableHandle(
                "sf1",
                "orders",
                1.0,
                TupleDomain.withColumnDomains(ImmutableMap.of(new TpchColumnHandle("c", BIGINT_ARRAY), Domain.singleValue(BIGINT_ARRAY, blockBuilder.build()))));

        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(original))).isEqualTo(original);
    }

    @Test
    public void testConstraintKeyedByForeignColumnIsRejected()
    {
        ColumnHandle foreign = new ColumnHandle() {};
        TpchTableHandle handle = new TpchTableHandle("sf1", "orders", 1.0, TupleDomain.withColumnDomains(ImmutableMap.of(foreign, Domain.singleValue(BIGINT, 1L))));

        assertInternalError(() -> provider.getTableHandleCodec().orElseThrow().serialize(handle))
                .hasMessageContaining("Tpch table constraint is keyed by");
    }

    @Test
    public void testIncompleteConstraintEntriesAreRejected()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        io.trino.plugin.tpch.protobuf.TpchColumnHandle column = io.trino.plugin.tpch.protobuf.TpchColumnHandle.newBuilder()
                .setColumnName("orderkey")
                .setTypeId(BIGINT.getTypeId().getId())
                .build();
        DomainProtoCodec domainProtoCodec = new DomainProtoCodec(TYPE_MANAGER, new TestingBlockEncodingSerde(TYPE_MANAGER));
        io.trino.protobuf.handles.v1.Domain domain = domainProtoCodec.toProto(Domain.singleValue(BIGINT, 1L));

        byte[] missingDomain = tableHandle(TpchTupleDomain.newBuilder()
                .setKind(TupleDomainKind.COLUMN_DOMAINS)
                .addColumnDomains(TpchColumnDomain.newBuilder().setColumn(column)));
        assertInternalError(() -> codec.deserialize(missingDomain))
                .hasMessage("Protobuf tpch table constraint has an entry without a column or a domain");

        byte[] repeatedColumn = tableHandle(TpchTupleDomain.newBuilder()
                .setKind(TupleDomainKind.COLUMN_DOMAINS)
                .addColumnDomains(TpchColumnDomain.newBuilder().setColumn(column).setDomain(domain))
                .addColumnDomains(TpchColumnDomain.newBuilder().setColumn(column).setDomain(domain)));
        assertInternalError(() -> codec.deserialize(repeatedColumn))
                .hasMessage("Protobuf tpch table constraint repeats column orderkey");

        byte[] unspecifiedKind = tableHandle(TpchTupleDomain.newBuilder());
        assertInternalError(() -> codec.deserialize(unspecifiedKind))
                .hasMessage("Invalid TupleDomain kind TUPLE_DOMAIN_KIND_UNSPECIFIED in tpch table constraint");

        // entries are ignored unless the kind says they are there
        byte[] noneWithEntries = tableHandle(TpchTupleDomain.newBuilder()
                .setKind(TupleDomainKind.NONE)
                .addColumnDomains(TpchColumnDomain.newBuilder().setColumn(column).setDomain(domain)));
        assertThat(codec.deserialize(noneWithEntries)).isEqualTo(new TpchTableHandle("sf1", "orders", 1.0, TupleDomain.none()));
    }

    @Test
    public void testCorruptBytesFailWithCategorizedError()
    {
        byte[] truncated = {0x0A, 0x05, 0x01};
        assertInternalError(() -> provider.getTableHandleCodec().orElseThrow().deserialize(truncated))
                .hasMessageContaining("Failed to decode protobuf TpchTableHandle");
        assertInternalError(() -> provider.getSplitCodec().orElseThrow().deserialize(truncated))
                .hasMessageContaining("Failed to decode protobuf TpchSplit");
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
    public void testUnknownColumnTypeIsRejected()
    {
        byte[] unknownType = io.trino.plugin.tpch.protobuf.TpchColumnHandle.newBuilder()
                .setColumnName("c")
                .setTypeId("no such type")
                .build()
                .toByteArray();
        assertInternalError(() -> provider.getColumnHandleCodec().orElseThrow().deserialize(unknownType))
                .hasMessage("Protobuf TpchColumnHandle does not describe a valid handle: Unknown type: no such type");
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
    public void testInvalidSplitAddressIsRejected()
    {
        byte[] invalidAddress = io.trino.plugin.tpch.protobuf.TpchSplit.newBuilder()
                .setPartNumber(1)
                .setTotalParts(2)
                .addAddresses("worker:not-a-port")
                .build()
                .toByteArray();
        assertInternalError(() -> provider.getSplitCodec().orElseThrow().deserialize(invalidAddress))
                .hasMessageContaining("Protobuf TpchSplit does not describe a valid handle");
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

    private static AbstractThrowableAssert<?, ? extends Throwable> assertInternalError(ThrowingCallable callable)
    {
        return assertThatThrownBy(callable)
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
    }

    private static byte[] tableHandle(TpchTupleDomain.Builder constraint)
    {
        return io.trino.plugin.tpch.protobuf.TpchTableHandle.newBuilder()
                .setSchemaName("sf1")
                .setTableName("orders")
                .setScaleFactor(1.0)
                .setConstraint(constraint)
                .build()
                .toByteArray();
    }
}
