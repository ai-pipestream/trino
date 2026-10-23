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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slices;
import io.trino.block.BlockJsonSerde;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveTypeName;
import io.trino.metastore.Partition;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.HiveWritableTableHandle.BucketInfo;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePageSinkMetadata;
import io.trino.plugin.hive.protobuf.BucketingVersion;
import io.trino.plugin.hive.protobuf.HiveColumnType;
import io.trino.plugin.hive.protobuf.HiveTupleDomain;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every Hive handle round-trips through its protobuf codec into an object
 * whose JSON form equals the original's, so the two control-plane encodings
 * carry the same information.
 */
public class TestHiveHandleCodecProvider
{
    private static final HiveColumnHandle ID = createBaseColumn("id", 0, HIVE_LONG, BIGINT, REGULAR, Optional.empty());
    private static final HiveColumnHandle NAME = createBaseColumn("name", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.of("the name"));
    private static final HiveColumnHandle DS = createBaseColumn("ds", -1, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle BUCKET_COLUMN = createBaseColumn("bucket_key", 2, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final AcidTransaction INSERT_TRANSACTION = new AcidTransaction(AcidOperation.INSERT, 17, 3);
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory(new JsonMapperProvider()
            .withJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
            .withJsonDeserializers(ImmutableMap.of(
                    Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE),
                    Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
            .get());

    private final HiveHandleCodecProvider provider = new HiveHandleCodecProvider(TESTING_TYPE_MANAGER, new DomainProtoCodec(TESTING_TYPE_MANAGER, TESTING_BLOCK_ENCODING_SERDE));

    @Test
    public void testColumnHandles()
    {
        Type rowType = RowType.from(ImmutableList.of(field("a", VARCHAR), field("b", BIGINT)));
        HiveColumnHandle projected = new HiveColumnHandle(
                "struct_col",
                3,
                toHiveType(rowType),
                rowType,
                Optional.of(new HiveColumnProjectionInfo(ImmutableList.of(1), ImmutableList.of("b"), HIVE_LONG, BIGINT)),
                REGULAR,
                Optional.of(""));
        for (HiveColumnHandle column : ImmutableList.of(ID, NAME, DS, projected, HiveColumnHandle.pathColumnHandle(), HiveColumnHandle.bucketColumnHandle(), HiveColumnHandle.mergeRowIdColumnHandle())) {
            assertRoundTrip(provider.getColumnHandleCodec().orElseThrow(), column, HiveColumnHandle.class);
        }
    }

    @Test
    public void testTableHandles()
    {
        HiveTableHandle minimal = new HiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);
        assertRoundTrip(provider.getTableHandleCodec().orElseThrow(), minimal, HiveTableHandle.class);

        HiveTableHandle none = new HiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(DS),
                ImmutableList.of(ID, NAME),
                TupleDomain.none(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of()),
                NO_ACID_TRANSACTION);
        assertRoundTrip(provider.getTableHandleCodec().orElseThrow(), none, HiveTableHandle.class);

        for (HivePartitioningHandle partitioningHandle : ImmutableList.of(
                new HivePartitioningHandle(BUCKETING_V2, 16, ImmutableList.of(HIVE_INT), false),
                new HiveUpdateHandle(BUCKETING_V1, 16, ImmutableList.of(HIVE_INT), true))) {
            HiveTableHandle full = new HiveTableHandle(
                    "schema",
                    "table",
                    ImmutableList.of(DS),
                    ImmutableList.of(ID, NAME, BUCKET_COLUMN),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            ID, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, false), Range.greaterThan(BIGINT, 1000L)), false),
                            NAME, Domain.create(ValueSet.of(VARCHAR, Slices.utf8Slice("a"), Slices.utf8Slice("b")).complement(), true))),
                    TupleDomain.withColumnDomains(ImmutableMap.of(DS, Domain.singleValue(VARCHAR, Slices.utf8Slice("2024-01-01")))),
                    Optional.of(new HiveTablePartitioning(
                            true,
                            ImmutableList.of(BUCKET_COLUMN),
                            partitioningHandle,
                            16,
                            ImmutableList.of(new SortingColumn("id", SortingColumn.Order.ASCENDING), new SortingColumn("name", SortingColumn.Order.DESCENDING)),
                            true)),
                    Optional.of(new HiveBucketFilter(ImmutableSet.of(1, 5, 9))),
                    Optional.of(ImmutableList.of(ImmutableList.of("2024-01-01"), ImmutableList.of("2024-01-02"), ImmutableList.of())),
                    INSERT_TRANSACTION);
            HiveTableHandle decoded = assertRoundTrip(provider.getTableHandleCodec().orElseThrow(), full, HiveTableHandle.class);
            assertThat(decoded.getTablePartitioning().orElseThrow().partitioningHandle().getClass()).isEqualTo(partitioningHandle.getClass());
        }
    }

    @Test
    public void testDiscreteDomainsRoundTripByValue()
    {
        // Domain.singleValue and ValueSet.of build run-length and dictionary encoded blocks tagged as discrete
        // sets. The codec rebuilds them the same way, so the handles are equal and re-encode to the same bytes.
        TupleDomain<HiveColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                DS, Domain.singleValue(VARCHAR, Slices.utf8Slice("2024-01-01")),
                ID, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true)));
        HiveTableHandle handle = new HiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(DS),
                ImmutableList.of(ID, NAME),
                predicate,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);

        ConnectorHandleCodec<ConnectorTableHandle> codec = provider.getTableHandleCodec().orElseThrow();
        HiveTableHandle decoded = (HiveTableHandle) codec.deserialize(codec.serialize(handle));

        assertThat(decoded.getCompactEffectivePredicate()).isEqualTo(predicate);
        for (Domain domain : decoded.getCompactEffectivePredicate().getDomains().orElseThrow().values()) {
            assertThat(domain.getValues().isDiscreteSet()).isTrue();
        }
        assertThat(codec.serialize(decoded)).isEqualTo(codec.serialize(handle));
    }

    @Test
    public void testTupleDomainMustBeKeyedByHiveColumns()
    {
        ColumnHandle foreign = new ColumnHandle() {};
        HiveTableHandle handle = new HiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(foreign, Domain.singleValue(BIGINT, 1L))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);
        assertTrinoExceptionThrownBy(() -> provider.getTableHandleCodec().orElseThrow().serialize(handle))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("keyed by");
    }

    @Test
    public void testSplits()
    {
        AcidInfo acidInfo = new AcidInfo(
                "file:///data/fullacid",
                ImmutableList.of("delete_delta_0000004_0000004_0000", "delete_delta_0000007_0000007_0000"),
                ImmutableList.of(new AcidInfo.OriginalFileInfo("000000_0", 1234)),
                7,
                true);
        HiveSplit full = new HiveSplit(
                "ds=2024-01-01",
                "file:///data/ds=2024-01-01/000000_0",
                42,
                87,
                88,
                1_700_000_000_000L,
                new Schema("org.apache.hadoop.hive.ql.io.orc.OrcSerde", true, ImmutableMap.of("foo", "bar", "columns", "id,name")),
                ImmutableList.of(new HivePartitionKey("ds", "2024-01-01"), new HivePartitionKey("empty", "")),
                OptionalInt.of(0),
                OptionalInt.of(3),
                true,
                ImmutableMap.of(1, new HiveTypeName("string"), 4, new HiveTypeName("bigint")),
                Optional.of(new BucketConversion(BUCKETING_V1, 32, 16, ImmutableList.of(BUCKET_COLUMN))),
                Optional.of(new BucketValidation(BUCKETING_V2, 32, ImmutableList.of(BUCKET_COLUMN, ID))),
                Optional.of(acidInfo),
                SplitWeight.fromProportion(2.0));
        HiveSplit decoded = assertRoundTrip(provider.getSplitCodec().orElseThrow(), full, HiveSplit.class);
        assertThat(decoded.getReadBucketNumber()).isEqualTo(OptionalInt.of(0));
        assertThat(decoded.getTableBucketNumber()).isEqualTo(OptionalInt.of(3));
        assertThat(decoded.getAcidInfo()).contains(acidInfo);
        assertThat(decoded.getSplitWeight()).isEqualTo(SplitWeight.fromProportion(2.0));
        assertThat(decoded.getHiveColumnCoercions()).isEqualTo(full.getHiveColumnCoercions());

        HiveSplit minimal = new HiveSplit(
                "",
                "file:///data/000000_0",
                0,
                0,
                0,
                0,
                new Schema("", false, ImmutableMap.of()),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                false,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                SplitWeight.standard());
        HiveSplit decodedMinimal = assertRoundTrip(provider.getSplitCodec().orElseThrow(), minimal, HiveSplit.class);
        assertThat(decodedMinimal.getReadBucketNumber()).isEmpty();
        assertThat(decodedMinimal.getBucketConversion()).isEmpty();
        assertThat(decodedMinimal.getAcidInfo()).isEmpty();
    }

    @Test
    public void testSplitWithoutSchemaIsRejected()
    {
        byte[] bytes = io.trino.plugin.hive.protobuf.HiveSplit.newBuilder().setPath("file:///x").build().toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getSplitCodec().orElseThrow().deserialize(bytes))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no schema");
    }

    @Test
    public void testWritableTableHandles()
    {
        Storage storage = new Storage(
                StorageFormat.createNullable("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
                Optional.of("file:///warehouse/schema/table"),
                Optional.of(new HiveBucketProperty(ImmutableList.of("bucket_key"), 16, ImmutableList.of(new SortingColumn("id", SortingColumn.Order.ASCENDING)))),
                false,
                ImmutableMap.of("serialization.format", "1"));
        Table table = new Table(
                "schema",
                "table",
                Optional.of("owner"),
                "MANAGED_TABLE",
                storage,
                ImmutableList.of(new Column("id", HIVE_LONG, Optional.of("id column"), ImmutableMap.of()), new Column("name", HIVE_STRING, Optional.empty(), ImmutableMap.of("k", "v"))),
                ImmutableList.of(new Column("ds", HIVE_STRING, Optional.empty(), ImmutableMap.of())),
                ImmutableMap.of("transactional", "true"),
                Optional.empty(),
                Optional.of("expanded"),
                OptionalLong.of(5));
        Partition partition = new Partition(
                "schema",
                "table",
                ImmutableList.of("2024-01-01"),
                new Storage(StorageFormat.createNullable(null, null, null), Optional.empty(), Optional.empty(), true, ImmutableMap.of()),
                ImmutableList.of(new Column("id", HIVE_LONG, Optional.empty(), ImmutableMap.of())),
                ImmutableMap.of());
        HivePageSinkMetadata pageSinkMetadata = new HivePageSinkMetadata(
                new SchemaTableName("schema", "table"),
                Optional.of(table),
                ImmutableMap.of(
                        ImmutableList.of("2024-01-01"), Optional.of(partition),
                        ImmutableList.of("2024-01-02"), Optional.empty()));
        LocationHandle locationHandle = new LocationHandle("file:///warehouse/schema/table", "file:///tmp/staging", WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        Optional<BucketInfo> bucketInfo = Optional.of(new BucketInfo(ImmutableList.of("bucket_key"), BUCKETING_V2, 16, ImmutableList.of(new SortingColumn("name", SortingColumn.Order.DESCENDING))));
        List<HiveColumnHandle> inputColumns = ImmutableList.of(ID, NAME, DS);

        HiveInsertTableHandle insert = new HiveInsertTableHandle(
                "schema", "table", inputColumns, pageSinkMetadata, locationHandle, bucketInfo, HiveStorageFormat.ORC, HiveStorageFormat.PARQUET, INSERT_TRANSACTION, true);
        assertRoundTrip(provider.getInsertTableHandleCodec().orElseThrow(), insert, HiveInsertTableHandle.class);

        HivePageSinkMetadata unpartitioned = new HivePageSinkMetadata(new SchemaTableName("schema", "new_table"), Optional.empty(), ImmutableMap.of());
        HiveOutputTableHandle output = new HiveOutputTableHandle(
                "schema",
                "new_table",
                inputColumns,
                unpartitioned,
                new LocationHandle("file:///warehouse/schema/new_table", "file:///warehouse/schema/new_table", WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY),
                HiveStorageFormat.ESRI_GEO_JSON,
                HiveStorageFormat.SEQUENCEFILE_PROTOBUF,
                ImmutableList.of("ds"),
                Optional.empty(),
                "owner",
                ImmutableMap.of("comment", "a table", "empty", ""),
                NO_ACID_TRANSACTION,
                true,
                false);
        assertRoundTrip(provider.getOutputTableHandleCodec().orElseThrow(), output, HiveOutputTableHandle.class);

        HiveTableExecuteHandle execute = new HiveTableExecuteHandle(
                "optimize",
                Optional.of("declaration-1"),
                Optional.of(0L),
                "schema",
                "table",
                inputColumns,
                pageSinkMetadata,
                locationHandle,
                Optional.empty(),
                HiveStorageFormat.ORC,
                HiveStorageFormat.ORC,
                INSERT_TRANSACTION,
                false);
        assertRoundTrip(provider.getTableExecuteHandleCodec().orElseThrow(), execute, HiveTableExecuteHandle.class);
        HiveTableExecuteHandle executeWithoutOptionals = new HiveTableExecuteHandle(
                "optimize",
                Optional.empty(),
                Optional.empty(),
                "schema",
                "table",
                inputColumns,
                pageSinkMetadata,
                locationHandle,
                Optional.empty(),
                HiveStorageFormat.ORC,
                HiveStorageFormat.ORC,
                NO_ACID_TRANSACTION,
                false);
        assertRoundTrip(provider.getTableExecuteHandleCodec().orElseThrow(), executeWithoutOptionals, HiveTableExecuteHandle.class);

        HiveTableHandle tableHandle = new HiveTableHandle(
                "schema", "table", ImmutableList.of(DS), inputColumns, TupleDomain.all(), TupleDomain.all(), Optional.empty(), Optional.empty(), Optional.empty(), new AcidTransaction(AcidOperation.MERGE, 21, 4));
        assertRoundTrip(provider.getMergeTableHandleCodec().orElseThrow(), new HiveMergeTableHandle(tableHandle, insert), HiveMergeTableHandle.class);
    }

    @Test
    public void testIncompleteWritableHandleIsRejected()
    {
        byte[] bytes = io.trino.plugin.hive.protobuf.HiveInsertTableHandle.newBuilder()
                .setWritable(io.trino.plugin.hive.protobuf.HiveWritableTableHandle.newBuilder().setSchemaName("s").setTableName("t"))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getInsertTableHandleCodec().orElseThrow().deserialize(bytes))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("insert table handle has no page sink metadata or no location handle");
    }

    @Test
    public void testTransactionHandle()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        for (HiveTransactionHandle handle : ImmutableList.of(new HiveTransactionHandle(true, UUID.randomUUID()), new HiveTransactionHandle(false, new UUID(0, 0)))) {
            assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);
        }
        byte[] invalid = io.trino.plugin.hive.protobuf.HiveTransactionHandle.newBuilder().setUuid("nope").build().toByteArray();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(invalid))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid uuid: nope");
    }

    @Test
    public void testPartitioningHandle()
    {
        ConnectorHandleCodec<ConnectorPartitioningHandle> codec = provider.getPartitioningHandleCodec().orElseThrow();
        for (HivePartitioningHandle handle : ImmutableList.of(
                new HivePartitioningHandle(BUCKETING_V1, 1, ImmutableList.of(), false),
                new HivePartitioningHandle(BUCKETING_V2, 1024, ImmutableList.of(HIVE_INT, HIVE_STRING, toHiveType(RowType.from(ImmutableList.of(field("a", BIGINT))))), true),
                new HiveUpdateHandle(BUCKETING_V2, 8, ImmutableList.of(HIVE_LONG), false))) {
            ConnectorPartitioningHandle decoded = codec.deserialize(codec.serialize(handle));
            assertThat(decoded).isEqualTo(handle);
            assertThat(decoded.getClass()).isEqualTo(handle.getClass());
        }
    }

    @Test
    public void testUnknownEnumValuesAreRejected()
    {
        byte[] bucketingVersion = io.trino.plugin.hive.protobuf.HivePartitioningHandle.newBuilder().setBucketCount(1).build().toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getPartitioningHandleCodec().orElseThrow().deserialize(bucketingVersion))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("unknown bucketing version: BUCKETING_VERSION_UNSPECIFIED");

        byte[] storageFormat = io.trino.plugin.hive.protobuf.HiveInsertTableHandle.newBuilder()
                .setWritable(io.trino.plugin.hive.protobuf.HiveWritableTableHandle.newBuilder()
                        .setPageSinkMetadata(io.trino.plugin.hive.protobuf.HivePageSinkMetadata.newBuilder().setSchemaName("s").setTableName("t"))
                        .setLocationHandle(io.trino.plugin.hive.protobuf.LocationHandle.newBuilder()
                                .setTargetPath("file:///t")
                                .setWritePath("file:///t")
                                .setWriteMode(io.trino.plugin.hive.protobuf.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY))
                        .setTableStorageFormatValue(99)
                        .setPartitionStorageFormat(io.trino.plugin.hive.protobuf.HiveStorageFormat.ORC)
                        .setTransaction(io.trino.plugin.hive.protobuf.AcidTransaction.newBuilder().setOperation(io.trino.plugin.hive.protobuf.AcidOperation.NONE)))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getInsertTableHandleCodec().orElseThrow().deserialize(storageFormat))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("unknown storage format: UNRECOGNIZED");
    }

    @Test
    public void testInvalidTypesAreRejected()
    {
        byte[] unknownType = io.trino.plugin.hive.protobuf.HiveColumnHandle.newBuilder()
                .setBaseColumnName("c")
                .setBaseHiveType("bigint")
                .setBaseTypeId("no_such_type")
                .setColumnType(HiveColumnType.REGULAR)
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getColumnHandleCodec().orElseThrow().deserialize(unknownType))
                .hasMessageContaining("no_such_type");

        byte[] invalidHiveType = io.trino.plugin.hive.protobuf.HiveColumnHandle.newBuilder()
                .setBaseColumnName("c")
                .setBaseHiveType("struct<")
                .setBaseTypeId("bigint")
                .setColumnType(HiveColumnType.REGULAR)
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getColumnHandleCodec().orElseThrow().deserialize(invalidHiveType))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid hive type: struct<");
    }

    @Test
    public void testInconsistentHandleIsCategorized()
    {
        // the bucketing columns do not match the partitioning handle types, which HiveTablePartitioning rejects
        io.trino.plugin.hive.protobuf.HiveColumnHandle column = io.trino.plugin.hive.protobuf.HiveColumnHandle.newBuilder()
                .setBaseColumnName("c")
                .setBaseHiveType("bigint")
                .setBaseTypeId("bigint")
                .setColumnType(HiveColumnType.REGULAR)
                .build();
        byte[] bytes = io.trino.plugin.hive.protobuf.HiveTableHandle.newBuilder()
                .setSchemaName("s")
                .setTableName("t")
                .setCompactEffectivePredicate(allDomains())
                .setEnforcedConstraint(allDomains())
                .setTransaction(io.trino.plugin.hive.protobuf.AcidTransaction.newBuilder().setOperation(io.trino.plugin.hive.protobuf.AcidOperation.NONE))
                .setTablePartitioning(io.trino.plugin.hive.protobuf.HiveTablePartitioning.newBuilder()
                        .addColumns(column)
                        .setPartitioningHandle(io.trino.plugin.hive.protobuf.HivePartitioningHandle.newBuilder()
                                .setBucketingVersion(BucketingVersion.BUCKETING_V2)
                                .setBucketCount(4)
                                .addHiveBucketTypes("int"))
                        .setTableBucketCount(4))
                .build()
                .toByteArray();
        assertTrinoExceptionThrownBy(() -> provider.getTableHandleCodec().orElseThrow().deserialize(bytes))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("does not describe a valid handle")
                .hasMessageContaining("columns do not match partitioning handle");
    }

    @Test
    public void testWrongHandleClassIsRejected()
    {
        ConnectorTableHandle foreign = new ConnectorTableHandle() {};
        assertTrinoExceptionThrownBy(() -> provider.getTableHandleCodec().orElseThrow().serialize(foreign))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("cannot encode");
    }

    @Test
    public void testCorruptBytesAreRejected()
    {
        byte[] corrupt = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertTrinoExceptionThrownBy(() -> provider.getSplitCodec().orElseThrow().deserialize(corrupt))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Failed to decode protobuf HiveSplit");
    }

    private static HiveTupleDomain allDomains()
    {
        return HiveTupleDomain.newBuilder().setKind(TupleDomainKind.ALL).build();
    }

    private static <F, H extends F> H assertRoundTrip(ConnectorHandleCodec<F> codec, H handle, Class<H> handleClass)
    {
        F decoded = codec.deserialize(codec.serialize(handle));
        assertThat(decoded).isInstanceOf(handleClass);
        JsonCodec<H> jsonCodec = JSON_CODEC_FACTORY.jsonCodec(handleClass);
        assertThat(jsonCodec.toJson(handleClass.cast(decoded))).isEqualTo(jsonCodec.toJson(handle));
        return handleClass.cast(decoded);
    }
}
