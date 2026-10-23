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
package io.trino.connector.informationschema;

import com.google.common.collect.ImmutableSet;
import io.trino.connector.v1.InformationSchemaTableName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInformationSchemaHandleCodecProvider
{
    private final InformationSchemaHandleCodecProvider provider = new InformationSchemaHandleCodecProvider();

    @Test
    public void testTableHandle()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        for (InformationSchemaTable table : InformationSchemaTable.values()) {
            InformationSchemaTableHandle handle = new InformationSchemaTableHandle(
                    "catalog",
                    table,
                    ImmutableSet.of(
                            new QualifiedTablePrefix("catalog"),
                            new QualifiedTablePrefix("catalog", "schema"),
                            new QualifiedTablePrefix("catalog", "schema", "table")),
                    OptionalLong.of(0));
            assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);
        }

        InformationSchemaTableHandle unlimited = new InformationSchemaTableHandle("catalog", InformationSchemaTable.TABLES, ImmutableSet.of(), OptionalLong.empty());
        assertThat(codec.deserialize(codec.serialize(unlimited))).isEqualTo(unlimited);

        InformationSchemaTableHandle maxLimit = new InformationSchemaTableHandle("catalog", InformationSchemaTable.TABLES, ImmutableSet.of(), OptionalLong.of(Long.MAX_VALUE));
        assertThat(codec.deserialize(codec.serialize(maxLimit))).isEqualTo(maxLimit);
    }

    @Test
    public void testEveryTableHasAWireName()
    {
        assertThat(InformationSchemaTableName.values())
                .filteredOn(name -> name != InformationSchemaTableName.UNRECOGNIZED && name != InformationSchemaTableName.INFORMATION_SCHEMA_TABLE_NAME_UNSPECIFIED)
                .extracting(InformationSchemaTableName::name)
                .containsExactlyInAnyOrder(Arrays.stream(InformationSchemaTable.values()).map(InformationSchemaTable::name).toArray(String[]::new));
    }

    @Test
    public void testMalformedTableHandlesAreRejected()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        byte[] unspecified = io.trino.connector.v1.InformationSchemaTableHandle.newBuilder()
                .setCatalogName("catalog")
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(unspecified))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("unknown table");

        byte[] unknownEnumValue = io.trino.connector.v1.InformationSchemaTableHandle.newBuilder()
                .setCatalogName("catalog")
                .setTableValue(999)
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(unknownEnumValue))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("unknown table");

        byte[] tableWithoutSchema = io.trino.connector.v1.InformationSchemaTableHandle.newBuilder()
                .setCatalogName("catalog")
                .setTable(InformationSchemaTableName.TABLES)
                .addPrefixes(io.trino.connector.v1.QualifiedTablePrefix.newBuilder().setCatalogName("catalog").setTableName("t"))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(tableWithoutSchema))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("table name without a schema name");
    }

    @Test
    public void testColumnHandle()
    {
        ConnectorHandleCodec<ColumnHandle> codec = provider.getColumnHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(new InformationSchemaColumnHandle("table_schema")))).isEqualTo(new InformationSchemaColumnHandle("table_schema"));
    }

    @Test
    public void testSplit()
    {
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        InformationSchemaSplit split = new InformationSchemaSplit(HostAddress.fromParts("coordinator.example", 8443));
        assertThat(codec.deserialize(codec.serialize(split))).isEqualTo(split);

        InformationSchemaSplit ipv6 = new InformationSchemaSplit(HostAddress.fromParts("::1", 8080));
        assertThat(codec.deserialize(codec.serialize(ipv6))).isEqualTo(ipv6);

        byte[] invalid = io.trino.connector.v1.InformationSchemaSplit.newBuilder().setAddress("host:not-a-port").build().toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(invalid))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid host address");
    }

    @Test
    public void testTransactionHandle()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        InformationSchemaTransactionHandle handle = new InformationSchemaTransactionHandle(TransactionId.create());
        assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);

        byte[] invalid = io.trino.connector.v1.InformationSchemaTransactionHandle.newBuilder().setTransactionId("").build().toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(invalid))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid transaction id");
    }

    @Test
    public void testCorruptBytesAreRejected()
    {
        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(new byte[] {(byte) 0xFF, (byte) 0xFF}))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Failed to decode protobuf InformationSchemaTableHandle");
    }
}
