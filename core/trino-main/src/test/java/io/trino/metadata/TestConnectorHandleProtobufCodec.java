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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import io.airlift.slice.Slices;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.informationschema.InformationSchemaColumnHandle;
import io.trino.connector.informationschema.InformationSchemaSplit;
import io.trino.connector.informationschema.InformationSchemaTable;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.connector.informationschema.InformationSchemaTransactionHandle;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.GlobalSystemTransactionHandle;
import io.trino.connector.system.SystemSplit;
import io.trino.connector.system.SystemTableHandle;
import io.trino.connector.system.SystemTransactionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionSplit;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchPartitioningHandle;
import io.trino.plugin.tpch.TpchSplit;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.server.protocol.v1.ConnectorHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.PlanTester;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createInformationSchemaCatalogHandle;
import static io.trino.connector.CatalogHandle.createSystemTablesCatalogHandle;
import static io.trino.metadata.HandleFamily.COLUMN;
import static io.trino.metadata.HandleFamily.PARTITIONING;
import static io.trino.metadata.HandleFamily.SPLIT;
import static io.trino.metadata.HandleFamily.TABLE;
import static io.trino.metadata.HandleFamily.TABLE_FUNCTION;
import static io.trino.metadata.HandleFamily.TRANSACTION;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Resolves codecs through a real catalog manager: tpch has codecs, the mock
 * connector has none, and every catalog also carries the engine-owned
 * information_schema and system connectors.
 */
@TestInstance(PER_CLASS)
public class TestConnectorHandleProtobufCodec
{
    private static final TransactionId TRANSACTION_ID = TransactionId.create();

    private PlanTester planTester;
    private ConnectorHandleProtobufCodec codec;
    private CatalogHandle tpch;
    private CatalogHandle mock;

    @BeforeAll
    public void setUp()
    {
        planTester = PlanTester.create(testSessionBuilder().build());
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        planTester.createCatalog("mock", MockConnectorFactory.create(), ImmutableMap.of());
        codec = new ConnectorHandleProtobufCodec(planTester.getConnectorServicesProvider());
        tpch = planTester.getCatalogHandle("tpch");
        mock = planTester.getCatalogHandle("mock");
    }

    @AfterAll
    public void tearDown()
    {
        planTester.close();
        planTester = null;
    }

    @Test
    public void testConnectorHandlesRoundTrip()
    {
        TpchTableHandle tableHandle = new TpchTableHandle("sf1", "orders", 1.0, TupleDomain.withColumnDomains(ImmutableMap.of(
                new TpchColumnHandle("orderkey", BIGINT), Domain.singleValue(BIGINT, 7L))));
        assertRoundTrip(tpch, TABLE, tableHandle);
        assertRoundTrip(tpch, COLUMN, new TpchColumnHandle("comment", VARCHAR));
        assertRoundTrip(tpch, SPLIT, new TpchSplit(2, 5, ImmutableList.of(HostAddress.fromParts("worker-1", 8080))));
        assertRoundTrip(tpch, TRANSACTION, TpchTransactionHandle.INSTANCE);
        assertRoundTrip(tpch, PARTITIONING, new TpchPartitioningHandle("orders", 1500000));
    }

    @Test
    public void testEncodedMessageNamesTheCatalog()
    {
        ConnectorHandle encoded = codec.encode(tpch, COLUMN, new TpchColumnHandle("comment", VARCHAR));
        assertThat(encoded.getCatalogHandle()).isEqualTo(tpch.getId());
        assertThat(encoded.getPayload().isEmpty()).isFalse();
    }

    @Test
    public void testInformationSchemaHandlesRoundTrip()
    {
        CatalogHandle informationSchema = createInformationSchemaCatalogHandle(tpch);
        assertRoundTrip(informationSchema, TABLE, new InformationSchemaTableHandle(
                "tpch",
                InformationSchemaTable.COLUMNS,
                ImmutableSet.of(new QualifiedTablePrefix("tpch", "sf1", "orders"), new QualifiedTablePrefix("tpch")),
                OptionalLong.of(10)));
        assertRoundTrip(informationSchema, COLUMN, new InformationSchemaColumnHandle("table_name"));
        assertRoundTrip(informationSchema, SPLIT, new InformationSchemaSplit(HostAddress.fromParts("coordinator", 8080)));
        assertRoundTrip(informationSchema, TRANSACTION, new InformationSchemaTransactionHandle(TRANSACTION_ID));
    }

    @Test
    public void testSystemHandlesRoundTrip()
    {
        CatalogHandle system = createSystemTablesCatalogHandle(tpch);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(
                new SystemColumnHandle("schema_name"), Domain.singleValue(VARCHAR, Slices.utf8Slice("sf1"))));
        assertRoundTrip(system, TABLE, new SystemTableHandle("metadata", "table_comments", constraint));
        assertRoundTrip(system, COLUMN, new SystemColumnHandle("schema_name"));
        SystemSplit split = new SystemSplit(ImmutableList.of(HostAddress.fromParts("worker-2", 8080)), constraint, Optional.of("tpch"));
        SystemSplit decodedSplit = (SystemSplit) codec.decode(SPLIT, codec.encode(system, SPLIT, split));
        assertThat(decodedSplit.getAddresses()).isEqualTo(split.getAddresses());
        assertThat(decodedSplit.getConstraint()).isEqualTo(constraint);
        assertThat(decodedSplit.getCatalogName()).isEqualTo(Optional.of("tpch"));

        // the nested transaction handle is encoded by the tpch codec
        SystemTransactionHandle transactionHandle = new SystemTransactionHandle(TRANSACTION_ID, TpchTransactionHandle.INSTANCE);
        SystemTransactionHandle decoded = (SystemTransactionHandle) codec.decode(TRANSACTION, codec.encode(system, TRANSACTION, transactionHandle));
        assertThat(decoded.getTransactionId()).isEqualTo(TRANSACTION_ID);
        assertThat(decoded.getConnectorTransactionHandle()).isEqualTo(TpchTransactionHandle.INSTANCE);
    }

    @Test
    public void testSystemTransactionOfCodecLessCatalogFailsLoudly()
    {
        CatalogHandle mockSystem = createSystemTablesCatalogHandle(mock);
        ConnectorTransactionHandle mockTransaction = new ConnectorTransactionHandle() {};
        assertTrinoExceptionThrownBy(() -> codec.encode(mockSystem, TRANSACTION, new SystemTransactionHandle(TRANSACTION_ID, mockTransaction)))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'mock'")
                .hasMessageContaining("transaction handles")
                .hasMessageContaining("no JSON fallback");
    }

    @Test
    public void testGlobalSystemHandlesRoundTrip()
    {
        CatalogHandle system = GlobalSystemConnector.CATALOG_HANDLE;
        assertRoundTrip(system, TRANSACTION, new GlobalSystemTransactionHandle(TRANSACTION_ID));
        assertRoundTrip(system, TABLE_FUNCTION, new SequenceFunctionHandle(-5, Long.MAX_VALUE, 3));
        SequenceFunctionSplit split = new SequenceFunctionSplit(10, 20);
        SequenceFunctionSplit decoded = (SequenceFunctionSplit) codec.decode(SPLIT, codec.encode(system, SPLIT, split));
        assertThat(decoded.getStart()).isEqualTo(10);
        assertThat(decoded.getStop()).isEqualTo(20);
    }

    @Test
    public void testCodecLessConnectorFailsLoudly()
    {
        MockConnectorTableHandle handle = new MockConnectorTableHandle(new SchemaTableName("default", "t"));
        assertTrinoExceptionThrownBy(() -> codec.encode(mock, TABLE, handle))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'mock'")
                .hasMessageContaining("table handles")
                .hasMessageContaining(MockConnectorTableHandle.class.getName())
                .hasMessageContaining("no JSON fallback");

        ConnectorHandle encoded = ConnectorHandle.newBuilder()
                .setCatalogHandle(mock.getId())
                .setPayload(ByteString.copyFrom(new byte[] {1}))
                .build();
        assertTrinoExceptionThrownBy(() -> codec.decode(TABLE, encoded))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'mock'")
                .hasMessageContaining("table handles");
    }

    @Test
    public void testUnknownCatalogIsRejected()
    {
        CatalogHandle unknown = CatalogHandle.createRootCatalogHandle(new CatalogName("nope"), new CatalogVersion("v1"));
        assertTrinoExceptionThrownBy(() -> codec.encode(unknown, COLUMN, new TpchColumnHandle("c", BIGINT)))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("nope");
        ConnectorHandle encoded = ConnectorHandle.newBuilder().setCatalogHandle(unknown.getId()).build();
        assertTrinoExceptionThrownBy(() -> codec.decode(COLUMN, encoded))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("nope");
    }

    @Test
    public void testInvalidCatalogHandleIdIsRejected()
    {
        ConnectorHandle encoded = ConnectorHandle.newBuilder().setCatalogHandle("not a catalog handle").build();
        assertTrinoExceptionThrownBy(() -> codec.decode(COLUMN, encoded))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid catalog handle id");
        assertTrinoExceptionThrownBy(() -> codec.decode(COLUMN, ConnectorHandle.getDefaultInstance()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid catalog handle id");
    }

    @Test
    public void testCorruptPayloadIsRejected()
    {
        ConnectorHandle encoded = ConnectorHandle.newBuilder()
                .setCatalogHandle(tpch.getId())
                .setPayload(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
                .build();
        assertTrinoExceptionThrownBy(() -> codec.decode(COLUMN, encoded))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Failed to decode protobuf");
    }

    @Test
    public void testMisbehavingCodecIsRejected()
    {
        CatalogHandle catalogHandle = CatalogHandle.createRootCatalogHandle(new CatalogName("broken"), new CatalogVersion("v1"));
        ConnectorHandleCodecProvider provider = new ConnectorHandleCodecProvider()
        {
            @Override
            public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
            {
                return Optional.of(new ConnectorHandleCodec<>()
                {
                    @Override
                    public byte[] serialize(ConnectorSplit handle)
                    {
                        return null;
                    }

                    @Override
                    public ConnectorSplit deserialize(byte[] bytes)
                    {
                        return null;
                    }
                });
            }
        };
        ConnectorHandleProtobufCodec brokenCodec = new ConnectorHandleProtobufCodec(singleCatalog(catalogHandle, provider));
        assertTrinoExceptionThrownBy(() -> brokenCodec.encode(catalogHandle, SPLIT, new TpchSplit(0, 1, ImmutableList.of())))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("returned null");
        assertTrinoExceptionThrownBy(() -> brokenCodec.decode(SPLIT, ConnectorHandle.newBuilder().setCatalogHandle(catalogHandle.getId()).build()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("decoded a split handle as null");
    }

    @Test
    public void testEveryFamilyIsListed()
    {
        assertThat(HandleFamily.all()).hasSize(14);
        assertThat(HandleFamily.all().stream().map(HandleFamily::name).distinct()).hasSize(14);
        ConnectorHandleCodecProvider empty = new ConnectorHandleCodecProvider() {};
        for (HandleFamily<?> family : HandleFamily.all()) {
            assertThat(family.codecFrom(empty)).isEmpty();
        }
    }

    private <T> void assertRoundTrip(CatalogHandle catalogHandle, HandleFamily<T> family, T handle)
    {
        ConnectorHandle encoded = codec.encode(catalogHandle, family, handle);
        assertThat(encoded.getCatalogHandle()).isEqualTo(catalogHandle.getId());
        T decoded = codec.decode(family, encoded);
        assertThat(decoded).isEqualTo(handle);
        assertThat(decoded.getClass()).isEqualTo(handle.getClass());
    }

    private static ConnectorServicesProvider singleCatalog(CatalogHandle catalogHandle, ConnectorHandleCodecProvider provider)
    {
        Connector connector = new Connector()
        {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<ConnectorHandleCodecProvider> getHandleCodecProvider()
            {
                return Optional.of(provider);
            }

            @Override
            public void shutdown() {}
        };
        ConnectorServices services = new ConnectorServices(noopTracer(), catalogHandle, connector);
        return new ConnectorServicesProvider()
        {
            @Override
            public void loadInitialCatalogs() {}

            @Override
            public void ensureCatalogsLoaded(List<CatalogProperties> catalogs) {}

            @Override
            public PrunableState getPrunableState()
            {
                return PrunableState.empty();
            }

            @Override
            public void pruneCatalogs(PrunableState prunableState, Set<CatalogHandle> catalogsInUse) {}

            @Override
            public ConnectorServices getConnectorServices(CatalogHandle handle)
            {
                if (!handle.equals(catalogHandle)) {
                    throw new IllegalArgumentException("No catalog " + handle);
                }
                return services;
            }
        };
    }
}
