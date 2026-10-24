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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaMetadata
{
    private static final SchemaTableName TABLE = new SchemaTableName("default", "orders");

    @Test
    public void testTableHandleKeepsItsDescription()
    {
        // a schema registry based supplier describes the table with the latest schema versions, which can
        // change between two lookups; this one returns a new version on every lookup
        VersionedTableDescriptionSupplier supplier = new VersionedTableDescriptionSupplier();
        KafkaConfig config = new KafkaConfig();
        KafkaMetadata metadata = new KafkaMetadata(config, supplier, new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config));

        KafkaTableHandle handle = metadata.getTableHandle(SESSION, TABLE, Optional.empty(), Optional.empty());
        assertThat(supplier.lookups()).isEqualTo(1);
        assertThat(handle.messageDataSchemaLocation()).contains("schema-registry-id:1;message=schema");
        assertThat(handle.columns()).extracting(KafkaColumnHandle::getName).contains("column_1");

        // the columns a query is analyzed and read with match the data schema of the handle
        assertThat(metadata.getTableSchema(SESSION, handle).getColumns())
                .extracting(ColumnSchema::getName)
                .contains("column_1", "_partition_id")
                .doesNotContain("column_2");
        assertThat(metadata.getTableSchema(SESSION, handle).getColumns())
                .filteredOn(column -> column.getName().equals("_partition_id"))
                .extracting(ColumnSchema::isHidden)
                .containsExactly(true);
        assertThat(metadata.getColumnHandles(SESSION, handle))
                .containsOnlyKeys(handle.columns().stream().map(KafkaColumnHandle::getName).toList())
                .containsKey("column_1");
        assertThat(supplier.lookups()).isEqualTo(1);

        // the table metadata is looked up again, and shows the latest version
        assertThat(metadata.getTableMetadata(SESSION, handle).getColumns())
                .extracting(ColumnMetadata::getName)
                .contains("column_2")
                .doesNotContain("column_1");
    }

    private static class VersionedTableDescriptionSupplier
            implements TableDescriptionSupplier
    {
        private final AtomicInteger version = new AtomicInteger();

        @Override
        public Set<SchemaTableName> listTables()
        {
            return ImmutableSet.of(TABLE);
        }

        @Override
        public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
        {
            int current = version.incrementAndGet();
            String column = "column_" + current;
            return Optional.of(new KafkaTopicDescription(
                    TABLE.getTableName(),
                    Optional.of(TABLE.getSchemaName()),
                    TABLE.getTableName(),
                    Optional.empty(),
                    Optional.of(new KafkaTopicFieldGroup(
                            "protobuf",
                            Optional.of("schema-registry-id:" + current + ";message=schema"),
                            Optional.of("orders-value"),
                            ImmutableList.of(new KafkaTopicFieldDescription(column, BIGINT, column, null, null, null, false))))));
        }

        public int lookups()
        {
            return version.get();
        }
    }
}
