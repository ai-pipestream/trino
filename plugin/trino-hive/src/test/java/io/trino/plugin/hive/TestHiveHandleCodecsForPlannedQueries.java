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
package io.trino.plugin.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slices;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.connector.CatalogHandle;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ConnectorHandleProtobufCodec;
import io.trino.metadata.HandleFamily;
import io.trino.metadata.Split;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Table;
import io.trino.server.protobuf.SplitProtoMapper;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.split.SplitSource;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.HandleFamily.COLUMN;
import static io.trino.metadata.HandleFamily.INSERT_TABLE;
import static io.trino.metadata.HandleFamily.MERGE_TABLE;
import static io.trino.metadata.HandleFamily.OUTPUT_TABLE;
import static io.trino.metadata.HandleFamily.PARTITIONING;
import static io.trino.metadata.HandleFamily.TABLE;
import static io.trino.metadata.HandleFamily.TABLE_EXECUTE;
import static io.trino.metadata.HandleFamily.TABLE_FUNCTION;
import static io.trino.metadata.HandleFamily.TRANSACTION;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Plans real queries against a Hive catalog and sends every connector handle
 * a coordinator would ship to a worker, the splits included, through the
 * protobuf handle codecs. Each decoded handle must serialize to the same JSON
 * as the original, so the two control-plane encodings carry the same
 * information for handles the connector actually produces, not only for
 * handles a test builds by hand.
 */
@TestInstance(PER_CLASS)
public class TestHiveHandleCodecsForPlannedQueries
{
    private static final String SCHEMA = "test_schema";
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("hive")
            .setSchema(SCHEMA)
            .setCatalogSessionProperty("hive", "non_transactional_optimize_enabled", "true")
            .build();
    private static final ObjectMapper OBJECT_MAPPER = new JsonMapperProvider()
            .withJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
            .get();

    private Path baseDir;
    private PlanTester planTester;
    private ConnectorHandleProtobufCodec codec;
    private SplitProtoMapper splitProtoMapper;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        baseDir = Files.createTempDirectory("hive-handle-codecs");
        planTester = PlanTester.create(SESSION);
        planTester.createCatalog("hive", new TestingHiveConnectorFactory(baseDir), Map.of());
        HiveMetastore metastore = getConnectorService(planTester, HiveMetastoreFactory.class).createMetastore(Optional.empty());
        metastore.createDatabase(Database.builder()
                .setDatabaseName(SCHEMA)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());
        codec = new ConnectorHandleProtobufCodec(planTester.getConnectorServicesProvider());
        splitProtoMapper = new SplitProtoMapper(codec);

        String rows = "SELECT " +
                "id, " +
                "CAST(id AS varchar) AS name, " +
                "CAST(id AS double) / 3 AS ratio, " +
                "CAST(id AS decimal(20, 2)) AS amount, " +
                "CAST(id AS decimal(5, 1)) AS score, " +
                "date_add('day', id, DATE '2024-01-01') AS day, " +
                "CAST(from_unixtime(id) AS timestamp(3)) AS created, " +
                "CAST(ROW(id, CAST(id AS varchar)) AS ROW(a bigint, b varchar)) AS nested, " +
                "ARRAY[id, id + 1] AS items, " +
                "CAST(id % 3 AS varchar) AS ds " +
                "FROM UNNEST(sequence(1, 30)) t(id)";
        planTester.executeStatement("CREATE TABLE partitioned WITH (partitioned_by = ARRAY['ds']) AS " + rows);
        // The local executor can neither write bucketed tables nor run DDL, so the bucketed tables go straight
        // into the metastore, with a data file of the partitioned table as their bucket 0. Only splits are
        // generated from them, so the rows need not match the bucket.
        Table partitioned = metastore.getTable(SCHEMA, "partitioned").orElseThrow();
        Partition partitionOne = metastore.getPartition(partitioned, ImmutableList.of("1")).orElseThrow();
        Path dataFile;
        try (Stream<Path> files = Files.list(localPath(partitionOne.getStorage().getLocation()))) {
            dataFile = files.filter(file -> !file.getFileName().toString().startsWith(".")).findFirst().orElseThrow();
        }
        String bucketedLocation = bucketedLocation("bucketed", dataFile);
        metastore.createTable(Table.builder(partitioned)
                .setTableName("bucketed")
                .setDataColumns(ImmutableList.<Column>builder()
                        .addAll(partitioned.getDataColumns())
                        .addAll(partitioned.getPartitionColumns())
                        .build())
                .setPartitionColumns(ImmutableList.of())
                .withStorage(storage -> storage
                        .setLocation(bucketedLocation)
                        .setBucketProperty(new HiveBucketProperty(
                                ImmutableList.of("id"),
                                4,
                                ImmutableList.of(new SortingColumn("id", SortingColumn.Order.ASCENDING)))))
                .build(), NO_PRIVILEGES);
        HiveBucketProperty nameBuckets = new HiveBucketProperty(ImmutableList.of("name"), 2, ImmutableList.of());
        String bucketedPartitionedLocation = bucketedLocation("bucketed_partitioned", Optional.empty());
        String bucketedPartitionLocation = bucketedLocation("bucketed_partitioned/ds=1", dataFile);
        metastore.createTable(Table.builder(partitioned)
                .setTableName("bucketed_partitioned")
                .withStorage(storage -> storage
                        .setLocation(bucketedPartitionedLocation)
                        .setBucketProperty(nameBuckets))
                .build(), NO_PRIVILEGES);
        metastore.addPartitions(SCHEMA, "bucketed_partitioned", ImmutableList.of(new PartitionWithStatistics(
                Partition.builder(partitionOne)
                        .setTableName("bucketed_partitioned")
                        .withStorage(storage -> storage
                                .setLocation(bucketedPartitionLocation)
                                .setBucketProperty(nameBuckets))
                        .build(),
                "ds=1",
                PartitionStatistics.empty())));
        planTester.executeStatement("CREATE TABLE text_table WITH (format = 'TEXTFILE') AS " + rows.replace("ARRAY[id, id + 1] AS items, ", ""));
    }

    private Path localPath(String location)
    {
        assertThat(location).startsWith("local:///");
        return baseDir.resolve(location.substring("local:///".length()));
    }

    // the file metastore requires managed tables to live in their schema directory
    private String bucketedLocation(String directory, Path bucketZero)
            throws IOException
    {
        return bucketedLocation(directory, Optional.of(bucketZero));
    }

    private String bucketedLocation(String directory, Optional<Path> bucketZero)
            throws IOException
    {
        String location = "local:///hive/%s/%s".formatted(SCHEMA, directory);
        Path path = Files.createDirectories(localPath(location));
        if (bucketZero.isPresent()) {
            Files.copy(bucketZero.get(), path.resolve("000000_0"));
        }
        return location;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        planTester.close();
        planTester = null;
        deleteRecursively(baseDir, ALLOW_INSECURE);
    }

    @Test
    public void testScans()
    {
        Set<String> handles = new HashSet<>();
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM partitioned"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM partitioned WHERE ds = '1' AND id > 5 AND ratio < 20.5 AND amount IN (1.00, 2.00, 7.00) " +
                "AND score BETWEEN 1.0 AND 9.5 AND day > DATE '2024-01-03' AND created < TIMESTAMP '1970-01-01 00:00:20.000' AND name LIKE '1%'"));
        handles.addAll(assertHandlesRoundTrip("SELECT id FROM partitioned WHERE ds IN ('0', '2') OR ds IS NULL"));
        handles.addAll(assertHandlesRoundTrip("SELECT nested.b, items[1] FROM partitioned WHERE nested.a = 3"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM text_table WHERE id <> 4 AND name NOT IN ('1', '2')"));
        handles.addAll(assertHandlesRoundTrip("SELECT \"$path\", \"$file_size\", \"$file_modified_time\", \"$partition\" FROM partitioned WHERE \"$path\" <> 'x' AND \"$file_size\" > 0"));
        assertThat(handles).contains(
                "table:HiveTableHandle",
                "column:HiveColumnHandle",
                "transaction:HiveTransactionHandle",
                "split:HiveSplit");
    }

    @Test
    public void testBucketedScans()
    {
        Set<String> handles = new HashSet<>();
        handles.addAll(assertHandlesRoundTrip("SELECT count(*) FROM bucketed WHERE id IN (1, 5, 9)"));
        handles.addAll(assertHandlesRoundTrip("SELECT \"$bucket\", id FROM bucketed WHERE \"$bucket\" = 1"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM bucketed a JOIN bucketed b ON a.id = b.id"));
        handles.addAll(assertHandlesRoundTrip("SELECT name, count(*) FROM bucketed_partitioned WHERE ds = '1' GROUP BY name"));
        assertThat(handles).contains(
                "table:HiveTableHandle",
                "split:HiveSplit",
                "partitioning:HivePartitioningHandle");
    }

    @Test
    public void testEngineOwnedHandles()
    {
        Set<String> handles = new HashSet<>();
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM \"partitioned$partitions\" WHERE ds = '1'"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM \"partitioned$properties\""));
        handles.addAll(assertHandlesRoundTrip("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'test_schema' AND table_name = 'partitioned'"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM information_schema.tables"));
        handles.addAll(assertHandlesRoundTrip("SELECT * FROM system.runtime.nodes"));
        assertThat(handles).contains(
                "table:SystemTableHandle",
                "split:SystemSplit",
                "transaction:SystemTransactionHandle",
                "table:InformationSchemaTableHandle",
                "split:InformationSchemaSplit",
                "transaction:InformationSchemaTransactionHandle");
    }

    @Test
    public void testWrites()
    {
        Set<String> handles = new HashSet<>();
        handles.addAll(assertHandlesRoundTrip("INSERT INTO partitioned SELECT * FROM partitioned WHERE ds = '1'"));
        handles.addAll(assertHandlesRoundTrip("INSERT INTO bucketed SELECT * FROM bucketed WHERE id < 10"));
        handles.addAll(assertHandlesRoundTrip("INSERT INTO bucketed_partitioned SELECT * FROM bucketed_partitioned"));
        handles.addAll(assertHandlesRoundTrip("CREATE TABLE created_partitioned WITH (partitioned_by = ARRAY['ds'], bucketed_by = ARRAY['id'], bucket_count = 2) AS SELECT * FROM partitioned"));
        handles.addAll(assertHandlesRoundTrip("CREATE TABLE created_text WITH (format = 'TEXTFILE') AS SELECT * FROM text_table"));
        handles.addAll(assertHandlesRoundTrip("ALTER TABLE partitioned EXECUTE optimize(file_size_threshold => '10MB')"));
        handles.addAll(assertHandlesRoundTrip("ANALYZE partitioned WITH (partitions = ARRAY[ARRAY['1'], ARRAY['2']])"));
        handles.addAll(assertHandlesRoundTrip("DELETE FROM partitioned WHERE ds = '2'"));
        assertThat(handles).contains(
                "insert table:HiveInsertTableHandle",
                "output table:HiveOutputTableHandle",
                "table execute:HiveTableExecuteHandle",
                "partitioning:HivePartitioningHandle");
    }

    private Set<String> assertHandlesRoundTrip(@Language("SQL") String sql)
    {
        Set<String> roundTripped = new HashSet<>();
        planTester.inTransaction(planTester.getDefaultSession(), session -> {
            // plan as the coordinator does for a cluster, so that remote fragments and their partitionings appear
            Plan plan = planTester.createPlan(session, sql, planTester.getPlanOptimizers(false), OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
            SubPlan subPlan = planTester.createSubPlans(session, plan, false);
            for (PlanFragment fragment : subPlan.getAllFragments()) {
                assertPartitioningRoundTrip(fragment.getPartitioning(), roundTripped);
                assertPartitioningRoundTrip(fragment.getOutputPartitioningScheme(), roundTripped);
                for (PlanNode node : PlanNodeSearcher.searchFrom(fragment.getRoot()).findAll()) {
                    switch (node) {
                        case TableScanNode scan -> {
                            assertTableRoundTrip(scan.getTable(), roundTripped);
                            for (ColumnHandle column : scan.getAssignments().values()) {
                                assertRoundTrip(scan.getTable().catalogHandle(), COLUMN, column, roundTripped);
                            }
                            SplitSource splits = planTester.getSplitManager().getSplits(session, Span.getInvalid(), scan.getTable(), EMPTY, alwaysTrue());
                            assertSplitsRoundTrip(splits, scan.getId(), roundTripped);
                        }
                        case TableFunctionProcessorNode function -> {
                            assertTableFunctionRoundTrip(function.getHandle(), roundTripped);
                            if (function.getSource().isEmpty()) {
                                SplitSource splits = planTester.getSplitManager().getSplits(session, Span.getInvalid(), function.getHandle());
                                assertSplitsRoundTrip(splits, function.getId(), roundTripped);
                            }
                        }
                        case TableFunctionNode function -> assertTableFunctionRoundTrip(function.getHandle(), roundTripped);
                        case TableWriterNode writer -> {
                            assertWriterTargetRoundTrip(writer.getTarget(), roundTripped);
                            writer.getPartitioningScheme().ifPresent(scheme -> assertPartitioningRoundTrip(scheme, roundTripped));
                        }
                        case TableExecuteNode execute -> {
                            assertWriterTargetRoundTrip(execute.getTarget(), roundTripped);
                            execute.getPartitioningScheme().ifPresent(scheme -> assertPartitioningRoundTrip(scheme, roundTripped));
                        }
                        case MergeWriterNode merge -> {
                            assertWriterTargetRoundTrip(merge.getTarget(), roundTripped);
                            merge.getPartitioningScheme().ifPresent(scheme -> assertPartitioningRoundTrip(scheme, roundTripped));
                        }
                        case MergeProcessorNode merge -> assertWriterTargetRoundTrip(merge.getTarget(), roundTripped);
                        case TableFinishNode finish -> assertWriterTargetRoundTrip(finish.getTarget(), roundTripped);
                        case TableDeleteNode delete -> assertTableRoundTrip(delete.getTarget(), roundTripped);
                        case ExchangeNode exchange -> assertPartitioningRoundTrip(exchange.getPartitioningScheme(), roundTripped);
                        default -> {}
                    }
                }
            }
            return null;
        });
        assertThat(roundTripped).as("handles shipped for %s", sql).isNotEmpty();
        return roundTripped;
    }

    private void assertTableRoundTrip(TableHandle table, Set<String> roundTripped)
    {
        assertRoundTrip(table.catalogHandle(), TABLE, table.connectorHandle(), roundTripped);
        assertRoundTrip(table.catalogHandle(), TRANSACTION, table.transaction(), roundTripped);
    }

    private void assertTableFunctionRoundTrip(TableFunctionHandle function, Set<String> roundTripped)
    {
        assertRoundTrip(function.catalogHandle(), TABLE_FUNCTION, function.functionHandle(), roundTripped);
        assertRoundTrip(function.catalogHandle(), TRANSACTION, function.transactionHandle(), roundTripped);
    }

    private void assertWriterTargetRoundTrip(WriterTarget target, Set<String> roundTripped)
    {
        switch (target) {
            case CreateTarget create -> {
                assertRoundTrip(create.getHandle().catalogHandle(), OUTPUT_TABLE, create.getHandle().connectorHandle(), roundTripped);
                assertRoundTrip(create.getHandle().catalogHandle(), TRANSACTION, create.getHandle().transactionHandle(), roundTripped);
            }
            case InsertTarget insert -> {
                assertRoundTrip(insert.getHandle().catalogHandle(), INSERT_TABLE, insert.getHandle().connectorHandle(), roundTripped);
                assertRoundTrip(insert.getHandle().catalogHandle(), TRANSACTION, insert.getHandle().transactionHandle(), roundTripped);
            }
            case TableExecuteTarget execute -> {
                assertRoundTrip(execute.getExecuteHandle().catalogHandle(), TABLE_EXECUTE, execute.getExecuteHandle().connectorHandle(), roundTripped);
                assertRoundTrip(execute.getExecuteHandle().catalogHandle(), TABLE, execute.getExecuteHandle().tableHandle(), roundTripped);
                execute.getSourceHandle().ifPresent(source -> assertTableRoundTrip(source, roundTripped));
            }
            case MergeTarget merge -> {
                assertTableRoundTrip(merge.getHandle(), roundTripped);
                merge.getMergeHandle().ifPresent(handle -> assertRoundTrip(handle.tableHandle().catalogHandle(), MERGE_TABLE, handle.connectorMergeHandle(), roundTripped));
            }
            default -> {}
        }
    }

    private void assertPartitioningRoundTrip(PartitioningScheme scheme, Set<String> roundTripped)
    {
        assertPartitioningRoundTrip(scheme.getPartitioning().getHandle(), roundTripped);
    }

    private void assertPartitioningRoundTrip(PartitioningHandle partitioning, Set<String> roundTripped)
    {
        partitioning.getCatalogHandle().ifPresent(catalogHandle -> {
            assertRoundTrip(catalogHandle, PARTITIONING, partitioning.getConnectorHandle(), roundTripped);
            partitioning.getTransactionHandle().ifPresent(transaction -> assertRoundTrip(catalogHandle, TRANSACTION, transaction, roundTripped));
        });
    }

    private void assertSplitsRoundTrip(SplitSource splitSource, PlanNodeId planNodeId, Set<String> roundTripped)
    {
        try (splitSource) {
            long sequenceId = 0;
            while (!splitSource.isFinished()) {
                for (Split split : getFutureValue(splitSource.getNextBatch(1000)).getSplits()) {
                    ScheduledSplit scheduled = new ScheduledSplit(sequenceId++, planNodeId, split);
                    ScheduledSplit decoded = splitProtoMapper.fromProto(splitProtoMapper.toProto(scheduled));
                    assertThat(decoded.sequenceId()).isEqualTo(scheduled.sequenceId());
                    assertThat(decoded.planNodeId()).isEqualTo(planNodeId);
                    assertThat(decoded.split().getCatalogHandle()).isEqualTo(split.getCatalogHandle());
                    assertSameJson(decoded.split().getConnectorSplit(), split.getConnectorSplit());
                    roundTripped.add("split:" + split.getConnectorSplit().getClass().getSimpleName());
                }
            }
        }
    }

    private <T> void assertRoundTrip(CatalogHandle catalogHandle, HandleFamily<T> family, T handle, Set<String> roundTripped)
    {
        T decoded = codec.decode(family, codec.encode(catalogHandle, family, handle));
        assertThat(decoded.getClass()).isEqualTo(handle.getClass());
        assertSameJson(decoded, handle);
        roundTripped.add(family + ":" + handle.getClass().getSimpleName());
    }

    private static void assertSameJson(Object decoded, Object original)
    {
        assertThat(jsonTree(decoded))
                .as("JSON of decoded %s", original.getClass().getSimpleName())
                .isEqualTo(jsonTree(original));
    }

    private static JsonNode jsonTree(Object value)
    {
        try {
            // The marker of a range set that is not discrete is a cache: the sender may have computed it as
            // NON_DISCRETE, the receiver recomputes it on first use.
            String json = OBJECT_MAPPER.writeValueAsString(value)
                    .replace("\"discreteSetMarker\":\"NON_DISCRETE\"", "\"discreteSetMarker\":\"UNKNOWN\"");
            JsonNode tree = OBJECT_MAPPER.readTree(json);
            replaceRangeBlocksWithValues(tree);
            return tree;
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    // A range set's block may be flat or a dictionary, depending on how it was built,
    // so range sets are compared by the values in their block
    private static void replaceRangeBlocksWithValues(JsonNode node)
    {
        if (node instanceof ObjectNode object && object.has("sortedRanges") && object.has("type")) {
            Type type = TESTING_TYPE_MANAGER.getType(TypeId.of(object.get("type").asText()));
            Block block = TESTING_BLOCK_ENCODING_SERDE.readBlock(Slices.wrappedBuffer(Base64.getDecoder().decode(object.get("sortedRanges").asText())).getInput());
            ArrayNode values = object.putArray("sortedRanges");
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(block.isNull(position) ? "null" : String.valueOf(type.getObjectValue(block, position)));
            }
        }
        node.forEach(TestHiveHandleCodecsForPlannedQueries::replaceRangeBlocksWithValues);
    }
}
