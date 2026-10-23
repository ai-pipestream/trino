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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.airlift.slice.Slices;
import io.trino.connector.CatalogHandle;
import io.trino.connector.v1.SystemColumnDomain;
import io.trino.connector.v1.SystemTupleDomain;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.server.protocol.v1.ConnectorHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSystemHandleCodecProvider
{
    private static final CatalogHandle CATALOG = CatalogHandle.createRootCatalogHandle(new CatalogName("served"), new CatalogVersion("v1"));
    private static final CatalogHandle OTHER_CATALOG = CatalogHandle.createRootCatalogHandle(new CatalogName("other"), new CatalogVersion("v1"));
    private static final DomainProtoCodec DOMAIN_CODEC = new DomainProtoCodec(TESTING_TYPE_MANAGER, TESTING_BLOCK_ENCODING_SERDE);
    private static final TransactionId TRANSACTION_ID = TransactionId.create();

    private record ServedTransactionHandle(String token)
            implements ConnectorTransactionHandle {}

    private static final ConnectorHandleCodecProvider SERVED_CATALOG_CODECS = new ConnectorHandleCodecProvider()
    {
        @Override
        public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
        {
            return Optional.of(new ConnectorHandleCodec<>()
            {
                @Override
                public byte[] serialize(ConnectorTransactionHandle handle)
                {
                    return ((ServedTransactionHandle) handle).token().getBytes(UTF_8);
                }

                @Override
                public ConnectorTransactionHandle deserialize(byte[] bytes)
                {
                    return new ServedTransactionHandle(new String(bytes, UTF_8));
                }
            });
        }
    };

    private final SystemHandleCodecProvider provider = new SystemHandleCodecProvider(CATALOG, Optional.of(SERVED_CATALOG_CODECS), DOMAIN_CODEC);

    @Test
    public void testTableHandle()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        for (TupleDomain<ColumnHandle> constraint : constraints()) {
            SystemTableHandle handle = new SystemTableHandle("runtime", "nodes", constraint);
            assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);
        }
    }

    @Test
    public void testColumnHandle()
    {
        ConnectorHandleCodec<ColumnHandle> codec = provider.getColumnHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(new SystemColumnHandle("node_id")))).isEqualTo(new SystemColumnHandle("node_id"));
        assertThat(codec.deserialize(codec.serialize(new SystemColumnHandle("")))).isEqualTo(new SystemColumnHandle(""));
    }

    @Test
    public void testSplit()
    {
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        for (TupleDomain<ColumnHandle> constraint : constraints()) {
            SystemSplit split = new SystemSplit(ImmutableList.of(HostAddress.fromParts("a", 1), HostAddress.fromParts("b", 2)), constraint, Optional.of("served"));
            SystemSplit decoded = (SystemSplit) codec.deserialize(codec.serialize(split));
            assertThat(decoded.getAddresses()).isEqualTo(split.getAddresses());
            assertThat(decoded.getConstraint()).isEqualTo(constraint);
            assertThat(decoded.getCatalogName()).isEqualTo(Optional.of("served"));
        }

        // a single address and no catalog name
        SystemSplit split = new SystemSplit(HostAddress.fromParts("a", 1), TupleDomain.all(), Optional.empty());
        SystemSplit decoded = (SystemSplit) codec.deserialize(codec.serialize(split));
        assertThat(decoded.getAddresses()).containsExactly(HostAddress.fromParts("a", 1));
        assertThat(decoded.getCatalogName()).isEmpty();
        assertThat(decoded.isRemotelyAccessible()).isEqualTo(split.isRemotelyAccessible());

        // a split always names at least one node
        byte[] noAddresses = io.trino.connector.v1.SystemSplit.newBuilder()
                .setConstraint(SystemTupleDomain.newBuilder().setKind(TupleDomainKind.ALL))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(noAddresses))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no addresses");
    }

    @Test
    public void testConstraintMustBeKeyedBySystemColumns()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        ColumnHandle foreign = new ColumnHandle() {};
        SystemTableHandle handle = new SystemTableHandle("runtime", "nodes", TupleDomain.withColumnDomains(ImmutableMap.of(foreign, Domain.singleValue(BIGINT, 1L))));
        assertTrinoExceptionThrownBy(() -> codec.serialize(handle))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("keyed by")
                .hasMessageContaining(SystemColumnHandle.class.getName());
    }

    @Test
    public void testMalformedConstraintsAreRejected()
    {
        assertTrinoExceptionThrownBy(() -> SystemHandleCodecProvider.fromProto(DOMAIN_CODEC, SystemTupleDomain.getDefaultInstance()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("TUPLE_DOMAIN_KIND_UNSPECIFIED")
                .hasMessageContaining("system table constraint");

        SystemTupleDomain missingDomain = SystemTupleDomain.newBuilder()
                .setKind(TupleDomainKind.COLUMN_DOMAINS)
                .addColumnDomains(SystemColumnDomain.newBuilder().setColumnName("c"))
                .build();
        assertTrinoExceptionThrownBy(() -> SystemHandleCodecProvider.fromProto(DOMAIN_CODEC, missingDomain))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no domain for column c");

        SystemColumnDomain domain = SystemColumnDomain.newBuilder()
                .setColumnName("c")
                .setDomain(DOMAIN_CODEC.toProto(Domain.singleValue(BIGINT, 1L)))
                .build();
        SystemTupleDomain repeated = SystemTupleDomain.newBuilder()
                .setKind(TupleDomainKind.COLUMN_DOMAINS)
                .addColumnDomains(domain)
                .addColumnDomains(domain)
                .build();
        assertTrinoExceptionThrownBy(() -> SystemHandleCodecProvider.fromProto(DOMAIN_CODEC, repeated))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("repeats a column");

        // ALL and NONE ignore stray column entries on the wire, like the Java TupleDomain does
        assertThat(SystemHandleCodecProvider.fromProto(DOMAIN_CODEC, SystemTupleDomain.newBuilder().setKind(TupleDomainKind.NONE).addColumnDomains(domain).build()).isNone()).isTrue();
    }

    @Test
    public void testTransactionHandle()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        SystemTransactionHandle handle = new SystemTransactionHandle(TRANSACTION_ID, new ServedTransactionHandle("tx-7"));
        byte[] bytes = codec.serialize(handle);
        SystemTransactionHandle decoded = (SystemTransactionHandle) codec.deserialize(bytes);
        assertThat(decoded.getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(decoded.getConnectorTransactionHandle()).isEqualTo(new ServedTransactionHandle("tx-7"));
        assertThat(decoded).isEqualTo(handle);
    }

    @Test
    public void testTransactionHandleWithoutServedCodecFailsLoudly()
    {
        SystemHandleCodecProvider codecLess = new SystemHandleCodecProvider(CATALOG, Optional.empty(), DOMAIN_CODEC);
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = codecLess.getTransactionHandleCodec().orElseThrow();
        SystemTransactionHandle handle = new SystemTransactionHandle(TRANSACTION_ID, new ServedTransactionHandle("tx-7"));
        assertTrinoExceptionThrownBy(() -> codec.serialize(handle))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'served'")
                .hasMessageContaining("transaction handles of type " + ServedTransactionHandle.class.getName());

        byte[] encodedElsewhere = provider.getTransactionHandleCodec().orElseThrow().serialize(handle);
        assertTrinoExceptionThrownBy(() -> codec.deserialize(encodedElsewhere))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'served'");
    }

    @Test
    public void testNestedTransactionHandleMustBelongToTheServedCatalog()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        byte[] foreign = io.trino.connector.v1.SystemTransactionHandle.newBuilder()
                .setTransactionId(TRANSACTION_ID.toString())
                .setConnectorTransactionHandle(ConnectorHandle.newBuilder()
                        .setCatalogHandle(OTHER_CATALOG.getId())
                        .setPayload(ByteString.copyFromUtf8("tx")))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(foreign))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("nests a transaction handle of catalog 'other'");

        byte[] missing = io.trino.connector.v1.SystemTransactionHandle.newBuilder()
                .setTransactionId(TRANSACTION_ID.toString())
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(missing))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no connector transaction handle");

        byte[] badTransactionId = io.trino.connector.v1.SystemTransactionHandle.newBuilder()
                .setTransactionId("not-a-uuid")
                .setConnectorTransactionHandle(ConnectorHandle.newBuilder()
                        .setCatalogHandle(CATALOG.getId())
                        .setPayload(ByteString.copyFromUtf8("tx")))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(badTransactionId))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid transaction id: not-a-uuid");
    }

    @Test
    public void testInvalidAddressIsRejected()
    {
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        byte[] bytes = io.trino.connector.v1.SystemSplit.newBuilder()
                .addAddresses("host:not-a-port")
                .setConstraint(SystemTupleDomain.newBuilder().setKind(TupleDomainKind.ALL))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(bytes))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid host address: host:not-a-port");
    }

    private static ImmutableList<TupleDomain<ColumnHandle>> constraints()
    {
        return ImmutableList.of(
                TupleDomain.all(),
                TupleDomain.none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new SystemColumnHandle("node_id"), Domain.singleValue(VARCHAR, Slices.utf8Slice("node-1")),
                        new SystemColumnHandle("coordinator"), Domain.singleValue(BOOLEAN, true),
                        new SystemColumnHandle("load"), Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 0.5), Range.lessThan(DOUBLE, -1.0)), true),
                        new SystemColumnHandle("count"), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
                        new SystemColumnHandle("absent"), Domain.onlyNull(BIGINT))));
    }
}
