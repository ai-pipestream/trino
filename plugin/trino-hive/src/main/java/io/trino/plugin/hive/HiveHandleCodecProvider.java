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
import com.google.inject.Inject;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveType;
import io.trino.metastore.HiveTypeName;
import io.trino.metastore.Partition;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.AcidInfo.OriginalFileInfo;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.HiveWritableTableHandle.BucketInfo;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePageSinkMetadata;
import io.trino.plugin.hive.protobuf.AnalyzePartitionValues;
import io.trino.plugin.hive.protobuf.HiveColumnDomain;
import io.trino.plugin.hive.protobuf.HiveColumnType;
import io.trino.plugin.hive.protobuf.HiveTupleDomain;
import io.trino.plugin.hive.protobuf.ModifiedPartition;
import io.trino.plugin.hive.protobuf.PartitionValues;
import io.trino.plugin.hive.protobuf.SortOrder;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.protobuf.handles.DomainProtoCodec.invalidTupleDomainKind;
import static io.trino.protobuf.handles.DomainProtoCodec.kindOf;
import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf handle codecs for every handle family the Hive connector ships
 * across the control plane: table, column, split, output, insert, table
 * execute, merge, partitioning and transaction handles. Each message mirrors
 * the JSON form of its handle, so the two encodings carry the same
 * information; pushed-down domains travel as Trino blocks through the shared
 * domain codec.
 */
public class HiveHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final TypeManager typeManager;
    private final DomainProtoCodec domainProtoCodec;

    private final ConnectorHandleCodec<ConnectorTableHandle> tableHandleCodec;
    private final ConnectorHandleCodec<ColumnHandle> columnHandleCodec;
    private final ConnectorHandleCodec<ConnectorSplit> splitCodec;
    private final ConnectorHandleCodec<ConnectorOutputTableHandle> outputTableHandleCodec;
    private final ConnectorHandleCodec<ConnectorInsertTableHandle> insertTableHandleCodec;
    private final ConnectorHandleCodec<ConnectorTableExecuteHandle> tableExecuteHandleCodec;
    private final ConnectorHandleCodec<ConnectorMergeTableHandle> mergeTableHandleCodec;
    private final ConnectorHandleCodec<ConnectorTransactionHandle> transactionHandleCodec;
    private final ConnectorHandleCodec<ConnectorPartitioningHandle> partitioningHandleCodec;

    @Inject
    public HiveHandleCodecProvider(TypeManager typeManager, DomainProtoCodec domainProtoCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.domainProtoCodec = requireNonNull(domainProtoCodec, "domainProtoCodec is null");

        tableHandleCodec = protobufHandleCodec(HiveTableHandle.class, io.trino.plugin.hive.protobuf.HiveTableHandle.parser(), this::toProto, this::fromProto);
        columnHandleCodec = protobufHandleCodec(HiveColumnHandle.class, io.trino.plugin.hive.protobuf.HiveColumnHandle.parser(), this::toProto, this::fromProto);
        splitCodec = protobufHandleCodec(HiveSplit.class, io.trino.plugin.hive.protobuf.HiveSplit.parser(), this::toProto, this::fromProto);
        outputTableHandleCodec = protobufHandleCodec(HiveOutputTableHandle.class, io.trino.plugin.hive.protobuf.HiveOutputTableHandle.parser(), this::toProto, this::fromProto);
        insertTableHandleCodec = protobufHandleCodec(HiveInsertTableHandle.class, io.trino.plugin.hive.protobuf.HiveInsertTableHandle.parser(), this::toProto, this::fromProto);
        tableExecuteHandleCodec = protobufHandleCodec(HiveTableExecuteHandle.class, io.trino.plugin.hive.protobuf.HiveTableExecuteHandle.parser(), this::toProto, this::fromProto);
        mergeTableHandleCodec = protobufHandleCodec(HiveMergeTableHandle.class, io.trino.plugin.hive.protobuf.HiveMergeTableHandle.parser(), this::toProto, this::fromProto);
        transactionHandleCodec = protobufHandleCodec(HiveTransactionHandle.class, io.trino.plugin.hive.protobuf.HiveTransactionHandle.parser(), HiveHandleCodecProvider::toProto, HiveHandleCodecProvider::fromProto);
        partitioningHandleCodec = protobufHandleCodec(HivePartitioningHandle.class, io.trino.plugin.hive.protobuf.HivePartitioningHandle.parser(), HiveHandleCodecProvider::toProto, HiveHandleCodecProvider::fromProto);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.of(tableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(columnHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(splitCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec()
    {
        return Optional.of(outputTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec()
    {
        return Optional.of(insertTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableExecuteHandle>> getTableExecuteHandleCodec()
    {
        return Optional.of(tableExecuteHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorMergeTableHandle>> getMergeTableHandleCodec()
    {
        return Optional.of(mergeTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(transactionHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorPartitioningHandle>> getPartitioningHandleCodec()
    {
        return Optional.of(partitioningHandleCodec);
    }

    // table handle

    private io.trino.plugin.hive.protobuf.HiveTableHandle toProto(HiveTableHandle handle)
    {
        io.trino.plugin.hive.protobuf.HiveTableHandle.Builder builder = io.trino.plugin.hive.protobuf.HiveTableHandle.newBuilder()
                .setSchemaName(handle.getSchemaName())
                .setTableName(handle.getTableName())
                .setCompactEffectivePredicate(toProto(handle.getCompactEffectivePredicate()))
                .setEnforcedConstraint(toProto(handle.getEnforcedConstraint()))
                .setTransaction(toProto(handle.getTransaction()));
        handle.getPartitionColumns().forEach(column -> builder.addPartitionColumns(toProto(column)));
        handle.getDataColumns().forEach(column -> builder.addDataColumns(toProto(column)));
        handle.getTablePartitioning().ifPresent(partitioning -> builder.setTablePartitioning(toProto(partitioning)));
        handle.getBucketFilter().ifPresent(filter -> builder.setBucketFilter(io.trino.plugin.hive.protobuf.HiveBucketFilter.newBuilder()
                .addAllBucketsToKeep(filter.bucketsToKeep())));
        handle.getAnalyzePartitionValues().ifPresent(partitions -> {
            AnalyzePartitionValues.Builder values = AnalyzePartitionValues.newBuilder();
            partitions.forEach(partition -> values.addPartitions(PartitionValues.newBuilder().addAllValues(partition)));
            builder.setAnalyzePartitionValues(values);
        });
        return builder.build();
    }

    private HiveTableHandle fromProto(io.trino.plugin.hive.protobuf.HiveTableHandle proto)
    {
        return new HiveTableHandle(
                proto.getSchemaName(),
                proto.getTableName(),
                columns(proto.getPartitionColumnsList()),
                columns(proto.getDataColumnsList()),
                fromProto(proto.getCompactEffectivePredicate(), "compact effective predicate"),
                fromProto(proto.getEnforcedConstraint(), "enforced constraint").transformKeys(ColumnHandle.class::cast),
                proto.hasTablePartitioning() ? Optional.of(fromProto(proto.getTablePartitioning())) : Optional.empty(),
                proto.hasBucketFilter() ? Optional.of(new HiveBucketFilter(ImmutableSet.copyOf(proto.getBucketFilter().getBucketsToKeepList()))) : Optional.empty(),
                proto.hasAnalyzePartitionValues()
                        ? Optional.of(proto.getAnalyzePartitionValues().getPartitionsList().stream()
                                      .<List<String>>map(partition -> partition.getValuesList())
                                      .collect(toImmutableList()))
                        : Optional.empty(),
                fromProto(proto.getTransaction()));
    }

    private HiveTupleDomain toProto(TupleDomain<? extends ColumnHandle> tupleDomain)
    {
        HiveTupleDomain.Builder builder = HiveTupleDomain.newBuilder()
                .setKind(kindOf(tupleDomain));
        tupleDomain.getDomains().ifPresent(domains -> domains.forEach((column, domain) -> {
            if (!(column instanceof HiveColumnHandle hiveColumn)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Hive tuple domain is keyed by " + column.getClass().getName() + " instead of " + HiveColumnHandle.class.getName());
            }
            builder.addColumnDomains(HiveColumnDomain.newBuilder()
                    .setColumn(toProto(hiveColumn))
                    .setDomain(domainProtoCodec.toProto(domain)));
        }));
        return builder.build();
    }

    private TupleDomain<HiveColumnHandle> fromProto(HiveTupleDomain proto, String context)
    {
        TupleDomainKind kind = proto.getKind();
        return switch (kind) {
            case ALL -> TupleDomain.all();
            case NONE -> TupleDomain.none();
            case COLUMN_DOMAINS -> {
                ImmutableMap.Builder<HiveColumnHandle, Domain> domains = ImmutableMap.builderWithExpectedSize(proto.getColumnDomainsCount());
                for (HiveColumnDomain columnDomain : proto.getColumnDomainsList()) {
                    if (!columnDomain.hasColumn() || !columnDomain.hasDomain()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive " + context + " has an entry without a column or a domain");
                    }
                    domains.put(fromProto(columnDomain.getColumn()), domainProtoCodec.fromProto(columnDomain.getDomain()));
                }
                Map<HiveColumnHandle, Domain> built;
                try {
                    built = domains.buildOrThrow();
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive " + context + " repeats a column", e);
                }
                yield TupleDomain.withColumnDomains(built);
            }
            case TUPLE_DOMAIN_KIND_UNSPECIFIED, UNRECOGNIZED -> throw invalidTupleDomainKind(kind, "hive " + context);
        };
    }

    // column handle

    private io.trino.plugin.hive.protobuf.HiveColumnHandle toProto(HiveColumnHandle column)
    {
        io.trino.plugin.hive.protobuf.HiveColumnHandle.Builder builder = io.trino.plugin.hive.protobuf.HiveColumnHandle.newBuilder()
                .setBaseColumnName(column.getBaseColumnName())
                .setBaseHiveColumnIndex(column.getBaseHiveColumnIndex())
                .setBaseHiveType(column.getBaseHiveType().toString())
                .setBaseTypeId(column.getBaseType().getTypeId().getId())
                .setColumnType(toProto(column.getColumnType()));
        column.getHiveColumnProjectionInfo().ifPresent(projection -> {
            builder.setProjection(io.trino.plugin.hive.protobuf.HiveColumnProjectionInfo.newBuilder()
                    .addAllDereferenceIndices(projection.getDereferenceIndices())
                    .addAllDereferenceNames(projection.getDereferenceNames())
                    .setHiveType(projection.getHiveType().toString())
                    .setTypeId(projection.getType().getTypeId().getId()));
        });
        column.getComment().ifPresent(builder::setComment);
        return builder.build();
    }

    private HiveColumnHandle fromProto(io.trino.plugin.hive.protobuf.HiveColumnHandle proto)
    {
        Optional<HiveColumnProjectionInfo> projection = Optional.empty();
        if (proto.hasProjection()) {
            io.trino.plugin.hive.protobuf.HiveColumnProjectionInfo projectionProto = proto.getProjection();
            projection = Optional.of(new HiveColumnProjectionInfo(
                    projectionProto.getDereferenceIndicesList(),
                    projectionProto.getDereferenceNamesList(),
                    hiveType(projectionProto.getHiveType()),
                    type(projectionProto.getTypeId())));
        }
        return new HiveColumnHandle(
                proto.getBaseColumnName(),
                proto.getBaseHiveColumnIndex(),
                hiveType(proto.getBaseHiveType()),
                type(proto.getBaseTypeId()),
                projection,
                fromProto(proto.getColumnType()),
                proto.hasComment() ? Optional.of(proto.getComment()) : Optional.empty());
    }

    private List<HiveColumnHandle> columns(List<io.trino.plugin.hive.protobuf.HiveColumnHandle> protos)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builderWithExpectedSize(protos.size());
        protos.forEach(column -> columns.add(fromProto(column)));
        return columns.build();
    }

    private static HiveColumnType toProto(ColumnType columnType)
    {
        return switch (columnType) {
            case PARTITION_KEY -> HiveColumnType.PARTITION_KEY;
            case REGULAR -> HiveColumnType.REGULAR;
            case SYNTHESIZED -> HiveColumnType.SYNTHESIZED;
        };
    }

    private static ColumnType fromProto(HiveColumnType columnType)
    {
        return switch (columnType) {
            case PARTITION_KEY -> ColumnType.PARTITION_KEY;
            case REGULAR -> ColumnType.REGULAR;
            case SYNTHESIZED -> ColumnType.SYNTHESIZED;
            case HIVE_COLUMN_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("column type", columnType);
        };
    }

    // partitioning

    private static io.trino.plugin.hive.protobuf.HivePartitioningHandle toProto(HivePartitioningHandle handle)
    {
        io.trino.plugin.hive.protobuf.HivePartitioningHandle.Builder builder = io.trino.plugin.hive.protobuf.HivePartitioningHandle.newBuilder()
                .setBucketingVersion(toProto(handle.getBucketingVersion()))
                .setBucketCount(handle.getBucketCount())
                .setUsePartitionedBucketing(handle.isUsePartitionedBucketing())
                .setUpdate(handle instanceof HiveUpdateHandle);
        handle.getHiveTypes().forEach(type -> builder.addHiveBucketTypes(type.toString()));
        return builder.build();
    }

    private static HivePartitioningHandle fromProto(io.trino.plugin.hive.protobuf.HivePartitioningHandle proto)
    {
        ImmutableList.Builder<HiveType> types = ImmutableList.builderWithExpectedSize(proto.getHiveBucketTypesCount());
        proto.getHiveBucketTypesList().forEach(type -> types.add(hiveType(type)));
        if (proto.getUpdate()) {
            return new HiveUpdateHandle(fromProto(proto.getBucketingVersion()), proto.getBucketCount(), types.build(), proto.getUsePartitionedBucketing());
        }
        return new HivePartitioningHandle(fromProto(proto.getBucketingVersion()), proto.getBucketCount(), types.build(), proto.getUsePartitionedBucketing());
    }

    private io.trino.plugin.hive.protobuf.HiveTablePartitioning toProto(HiveTablePartitioning partitioning)
    {
        io.trino.plugin.hive.protobuf.HiveTablePartitioning.Builder builder = io.trino.plugin.hive.protobuf.HiveTablePartitioning.newBuilder()
                .setActive(partitioning.active())
                .setPartitioningHandle(toProto(partitioning.partitioningHandle()))
                .setTableBucketCount(partitioning.tableBucketCount())
                .setForWrite(partitioning.forWrite());
        partitioning.columns().forEach(column -> builder.addColumns(toProto(column)));
        partitioning.sortedBy().forEach(column -> builder.addSortedBy(toProto(column)));
        return builder.build();
    }

    private HiveTablePartitioning fromProto(io.trino.plugin.hive.protobuf.HiveTablePartitioning proto)
    {
        if (!proto.hasPartitioningHandle()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive table partitioning has no partitioning handle");
        }
        return new HiveTablePartitioning(
                proto.getActive(),
                columns(proto.getColumnsList()),
                fromProto(proto.getPartitioningHandle()),
                proto.getTableBucketCount(),
                sortingColumns(proto.getSortedByList()),
                proto.getForWrite());
    }

    private static io.trino.plugin.hive.protobuf.BucketingVersion toProto(BucketingVersion version)
    {
        return switch (version) {
            case BUCKETING_V1 -> io.trino.plugin.hive.protobuf.BucketingVersion.BUCKETING_V1;
            case BUCKETING_V2 -> io.trino.plugin.hive.protobuf.BucketingVersion.BUCKETING_V2;
        };
    }

    private static BucketingVersion fromProto(io.trino.plugin.hive.protobuf.BucketingVersion version)
    {
        return switch (version) {
            case BUCKETING_V1 -> BucketingVersion.BUCKETING_V1;
            case BUCKETING_V2 -> BucketingVersion.BUCKETING_V2;
            case BUCKETING_VERSION_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("bucketing version", version);
        };
    }

    private static io.trino.plugin.hive.protobuf.SortingColumn toProto(SortingColumn column)
    {
        return io.trino.plugin.hive.protobuf.SortingColumn.newBuilder()
                .setColumnName(column.columnName())
                .setOrder(switch (column.order()) {
                    case ASCENDING -> SortOrder.ASCENDING;
                    case DESCENDING -> SortOrder.DESCENDING;
                })
                .build();
    }

    private static SortingColumn fromProto(io.trino.plugin.hive.protobuf.SortingColumn proto)
    {
        SortingColumn.Order order = switch (proto.getOrder()) {
            case ASCENDING -> SortingColumn.Order.ASCENDING;
            case DESCENDING -> SortingColumn.Order.DESCENDING;
            case SORT_ORDER_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("sort order", proto.getOrder());
        };
        return new SortingColumn(proto.getColumnName(), order);
    }

    private static List<SortingColumn> sortingColumns(List<io.trino.plugin.hive.protobuf.SortingColumn> protos)
    {
        ImmutableList.Builder<SortingColumn> columns = ImmutableList.builderWithExpectedSize(protos.size());
        protos.forEach(column -> columns.add(fromProto(column)));
        return columns.build();
    }

    // acid

    private static io.trino.plugin.hive.protobuf.AcidTransaction toProto(AcidTransaction transaction)
    {
        return io.trino.plugin.hive.protobuf.AcidTransaction.newBuilder()
                .setOperation(switch (transaction.getOperation()) {
                    case NONE -> io.trino.plugin.hive.protobuf.AcidOperation.NONE;
                    case CREATE_TABLE -> io.trino.plugin.hive.protobuf.AcidOperation.CREATE_TABLE;
                    case INSERT -> io.trino.plugin.hive.protobuf.AcidOperation.INSERT;
                    case MERGE -> io.trino.plugin.hive.protobuf.AcidOperation.MERGE;
                })
                .setTransactionId(transaction.getAcidTransactionIdForSerialization())
                .setWriteId(transaction.getWriteIdForSerialization())
                .build();
    }

    private static AcidTransaction fromProto(io.trino.plugin.hive.protobuf.AcidTransaction proto)
    {
        AcidOperation operation = switch (proto.getOperation()) {
            case NONE -> AcidOperation.NONE;
            case CREATE_TABLE -> AcidOperation.CREATE_TABLE;
            case INSERT -> AcidOperation.INSERT;
            case MERGE -> AcidOperation.MERGE;
            case ACID_OPERATION_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("acid operation", proto.getOperation());
        };
        return new AcidTransaction(operation, proto.getTransactionId(), proto.getWriteId());
    }

    // split

    private io.trino.plugin.hive.protobuf.HiveSplit toProto(HiveSplit split)
    {
        io.trino.plugin.hive.protobuf.HiveSplit.Builder builder = io.trino.plugin.hive.protobuf.HiveSplit.newBuilder()
                .setPartitionName(split.getPartitionName())
                .setPath(split.getPath())
                .setStart(split.getStart())
                .setLength(split.getLength())
                .setEstimatedFileSize(split.getEstimatedFileSize())
                .setFileModifiedTime(split.getFileModifiedTime())
                .setSchema(io.trino.plugin.hive.protobuf.Schema.newBuilder()
                        .setSerializationLibraryName(split.getSchema().serializationLibraryName())
                        .setFullAcidTable(split.getSchema().isFullAcidTable())
                        .putAllSerdeProperties(split.getSchema().serdeProperties()))
                .setForceLocalScheduling(split.isForceLocalScheduling())
                .setSplitWeight(split.getSplitWeight().getRawValue());
        split.getPartitionKeys().forEach(key -> builder.addPartitionKeys(io.trino.plugin.hive.protobuf.HivePartitionKey.newBuilder()
                .setName(key.name())
                .setValue(key.value())));
        split.getReadBucketNumber().ifPresent(builder::setReadBucketNumber);
        split.getTableBucketNumber().ifPresent(builder::setTableBucketNumber);
        split.getHiveColumnCoercions().forEach((index, typeName) -> builder.putHiveColumnCoercions(index, typeName.toString()));
        split.getBucketConversion().ifPresent(conversion -> {
            io.trino.plugin.hive.protobuf.BucketConversion.Builder conversionBuilder = io.trino.plugin.hive.protobuf.BucketConversion.newBuilder()
                    .setBucketingVersion(toProto(conversion.bucketingVersion()))
                    .setTableBucketCount(conversion.tableBucketCount())
                    .setPartitionBucketCount(conversion.partitionBucketCount());
            conversion.bucketColumnHandles().forEach(column -> conversionBuilder.addBucketColumnHandles(toProto(column)));
            builder.setBucketConversion(conversionBuilder);
        });
        split.getBucketValidation().ifPresent(validation -> {
            io.trino.plugin.hive.protobuf.BucketValidation.Builder validationBuilder = io.trino.plugin.hive.protobuf.BucketValidation.newBuilder()
                    .setBucketingVersion(toProto(validation.bucketingVersion()))
                    .setBucketCount(validation.bucketCount());
            validation.bucketColumns().forEach(column -> validationBuilder.addBucketColumns(toProto(column)));
            builder.setBucketValidation(validationBuilder);
        });
        split.getAcidInfo().ifPresent(acidInfo -> {
            io.trino.plugin.hive.protobuf.AcidInfo.Builder acidBuilder = io.trino.plugin.hive.protobuf.AcidInfo.newBuilder()
                    .setPartitionLocation(acidInfo.partitionLocation())
                    .addAllDeleteDeltaDirectories(acidInfo.deleteDeltaDirectories())
                    .setBucketId(acidInfo.bucketId())
                    .setOrcAcidVersionValidated(acidInfo.orcAcidVersionValidated());
            acidInfo.originalFiles().forEach(file -> acidBuilder.addOriginalFiles(io.trino.plugin.hive.protobuf.OriginalFileInfo.newBuilder()
                    .setName(file.name())
                    .setFileSize(file.fileSize())));
            builder.setAcidInfo(acidBuilder);
        });
        return builder.build();
    }

    private HiveSplit fromProto(io.trino.plugin.hive.protobuf.HiveSplit proto)
    {
        if (!proto.hasSchema()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive split has no schema");
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builderWithExpectedSize(proto.getPartitionKeysCount());
        proto.getPartitionKeysList().forEach(key -> partitionKeys.add(new HivePartitionKey(key.getName(), key.getValue())));
        ImmutableMap.Builder<Integer, HiveTypeName> coercions = ImmutableMap.builderWithExpectedSize(proto.getHiveColumnCoercionsCount());
        proto.getHiveColumnCoercionsMap().forEach((index, typeName) -> coercions.put(index, new HiveTypeName(typeName)));

        Optional<BucketConversion> bucketConversion = Optional.empty();
        if (proto.hasBucketConversion()) {
            io.trino.plugin.hive.protobuf.BucketConversion conversion = proto.getBucketConversion();
            bucketConversion = Optional.of(new BucketConversion(
                    fromProto(conversion.getBucketingVersion()),
                    conversion.getTableBucketCount(),
                    conversion.getPartitionBucketCount(),
                    columns(conversion.getBucketColumnHandlesList())));
        }
        Optional<BucketValidation> bucketValidation = Optional.empty();
        if (proto.hasBucketValidation()) {
            io.trino.plugin.hive.protobuf.BucketValidation validation = proto.getBucketValidation();
            bucketValidation = Optional.of(new BucketValidation(
                    fromProto(validation.getBucketingVersion()),
                    validation.getBucketCount(),
                    columns(validation.getBucketColumnsList())));
        }
        Optional<AcidInfo> acidInfo = Optional.empty();
        if (proto.hasAcidInfo()) {
            io.trino.plugin.hive.protobuf.AcidInfo info = proto.getAcidInfo();
            ImmutableList.Builder<OriginalFileInfo> originalFiles = ImmutableList.builderWithExpectedSize(info.getOriginalFilesCount());
            info.getOriginalFilesList().forEach(file -> originalFiles.add(new OriginalFileInfo(file.getName(), file.getFileSize())));
            acidInfo = Optional.of(new AcidInfo(
                    info.getPartitionLocation(),
                    info.getDeleteDeltaDirectoriesList(),
                    originalFiles.build(),
                    info.getBucketId(),
                    info.getOrcAcidVersionValidated()));
        }
        return new HiveSplit(
                proto.getPartitionName(),
                proto.getPath(),
                proto.getStart(),
                proto.getLength(),
                proto.getEstimatedFileSize(),
                proto.getFileModifiedTime(),
                new Schema(proto.getSchema().getSerializationLibraryName(), proto.getSchema().getFullAcidTable(), proto.getSchema().getSerdePropertiesMap()),
                partitionKeys.build(),
                proto.hasReadBucketNumber() ? OptionalInt.of(proto.getReadBucketNumber()) : OptionalInt.empty(),
                proto.hasTableBucketNumber() ? OptionalInt.of(proto.getTableBucketNumber()) : OptionalInt.empty(),
                proto.getForceLocalScheduling(),
                coercions.buildOrThrow(),
                bucketConversion,
                bucketValidation,
                acidInfo,
                splitWeight(proto.getSplitWeight()));
    }

    // writable table handles

    private io.trino.plugin.hive.protobuf.HiveWritableTableHandle toProtoWritable(HiveWritableTableHandle handle)
    {
        io.trino.plugin.hive.protobuf.HiveWritableTableHandle.Builder builder = io.trino.plugin.hive.protobuf.HiveWritableTableHandle.newBuilder()
                .setSchemaName(handle.getSchemaName())
                .setTableName(handle.getTableName())
                .setPageSinkMetadata(toProto(handle.getPageSinkMetadata()))
                .setLocationHandle(toProto(handle.getLocationHandle()))
                .setTableStorageFormat(toProto(handle.getTableStorageFormat()))
                .setPartitionStorageFormat(toProto(handle.getPartitionStorageFormat()))
                .setTransaction(toProto(handle.getTransaction()))
                .setRetriesEnabled(handle.isRetriesEnabled());
        handle.getInputColumns().forEach(column -> builder.addInputColumns(toProto(column)));
        handle.getBucketInfo().ifPresent(bucketInfo -> {
            io.trino.plugin.hive.protobuf.BucketInfo.Builder bucketBuilder = io.trino.plugin.hive.protobuf.BucketInfo.newBuilder()
                    .addAllBucketedBy(bucketInfo.bucketedBy())
                    .setBucketingVersion(toProto(bucketInfo.bucketingVersion()))
                    .setBucketCount(bucketInfo.bucketCount());
            bucketInfo.sortedBy().forEach(column -> bucketBuilder.addSortedBy(toProto(column)));
            builder.setBucketInfo(bucketBuilder);
        });
        return builder.build();
    }

    private record WritableParts(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> inputColumns,
            HivePageSinkMetadata pageSinkMetadata,
            LocationHandle locationHandle,
            Optional<BucketInfo> bucketInfo,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            AcidTransaction transaction,
            boolean retriesEnabled) {}

    private WritableParts fromProtoWritable(io.trino.plugin.hive.protobuf.HiveWritableTableHandle proto, String handleName)
    {
        if (!proto.hasPageSinkMetadata() || !proto.hasLocationHandle()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Protobuf hive %s has no page sink metadata or no location handle", handleName));
        }
        Optional<BucketInfo> bucketInfo = Optional.empty();
        if (proto.hasBucketInfo()) {
            io.trino.plugin.hive.protobuf.BucketInfo info = proto.getBucketInfo();
            bucketInfo = Optional.of(new BucketInfo(
                    info.getBucketedByList(),
                    fromProto(info.getBucketingVersion()),
                    info.getBucketCount(),
                    sortingColumns(info.getSortedByList())));
        }
        return new WritableParts(
                proto.getSchemaName(),
                proto.getTableName(),
                columns(proto.getInputColumnsList()),
                fromProto(proto.getPageSinkMetadata()),
                fromProto(proto.getLocationHandle()),
                bucketInfo,
                fromProto(proto.getTableStorageFormat()),
                fromProto(proto.getPartitionStorageFormat()),
                fromProto(proto.getTransaction()),
                proto.getRetriesEnabled());
    }

    private io.trino.plugin.hive.protobuf.HiveInsertTableHandle toProto(HiveInsertTableHandle handle)
    {
        return io.trino.plugin.hive.protobuf.HiveInsertTableHandle.newBuilder()
                .setWritable(toProtoWritable(handle))
                .build();
    }

    private HiveInsertTableHandle fromProto(io.trino.plugin.hive.protobuf.HiveInsertTableHandle proto)
    {
        WritableParts parts = fromProtoWritable(proto.getWritable(), "insert table handle");
        return new HiveInsertTableHandle(
                parts.schemaName(),
                parts.tableName(),
                parts.inputColumns(),
                parts.pageSinkMetadata(),
                parts.locationHandle(),
                parts.bucketInfo(),
                parts.tableStorageFormat(),
                parts.partitionStorageFormat(),
                parts.transaction(),
                parts.retriesEnabled());
    }

    private io.trino.plugin.hive.protobuf.HiveOutputTableHandle toProto(HiveOutputTableHandle handle)
    {
        return io.trino.plugin.hive.protobuf.HiveOutputTableHandle.newBuilder()
                .setWritable(toProtoWritable(handle))
                .addAllPartitionedBy(handle.getPartitionedBy())
                .setTableOwner(handle.getTableOwner())
                .putAllAdditionalTableParameters(handle.getAdditionalTableParameters())
                .setExternal(handle.isExternal())
                .build();
    }

    private HiveOutputTableHandle fromProto(io.trino.plugin.hive.protobuf.HiveOutputTableHandle proto)
    {
        WritableParts parts = fromProtoWritable(proto.getWritable(), "output table handle");
        return new HiveOutputTableHandle(
                parts.schemaName(),
                parts.tableName(),
                parts.inputColumns(),
                parts.pageSinkMetadata(),
                parts.locationHandle(),
                parts.tableStorageFormat(),
                parts.partitionStorageFormat(),
                proto.getPartitionedByList(),
                parts.bucketInfo(),
                proto.getTableOwner(),
                proto.getAdditionalTableParametersMap(),
                parts.transaction(),
                proto.getExternal(),
                parts.retriesEnabled());
    }

    private io.trino.plugin.hive.protobuf.HiveTableExecuteHandle toProto(HiveTableExecuteHandle handle)
    {
        io.trino.plugin.hive.protobuf.HiveTableExecuteHandle.Builder builder = io.trino.plugin.hive.protobuf.HiveTableExecuteHandle.newBuilder()
                .setWritable(toProtoWritable(handle))
                .setProcedureName(handle.getProcedureName());
        handle.getWriteDeclarationId().ifPresent(builder::setWriteDeclarationId);
        handle.getMaxScannedFileSize().ifPresent(builder::setMaxScannedFileSize);
        return builder.build();
    }

    private HiveTableExecuteHandle fromProto(io.trino.plugin.hive.protobuf.HiveTableExecuteHandle proto)
    {
        WritableParts parts = fromProtoWritable(proto.getWritable(), "table execute handle");
        return new HiveTableExecuteHandle(
                proto.getProcedureName(),
                proto.hasWriteDeclarationId() ? Optional.of(proto.getWriteDeclarationId()) : Optional.empty(),
                proto.hasMaxScannedFileSize() ? Optional.of(proto.getMaxScannedFileSize()) : Optional.empty(),
                parts.schemaName(),
                parts.tableName(),
                parts.inputColumns(),
                parts.pageSinkMetadata(),
                parts.locationHandle(),
                parts.bucketInfo(),
                parts.tableStorageFormat(),
                parts.partitionStorageFormat(),
                parts.transaction(),
                parts.retriesEnabled());
    }

    private io.trino.plugin.hive.protobuf.HiveMergeTableHandle toProto(HiveMergeTableHandle handle)
    {
        return io.trino.plugin.hive.protobuf.HiveMergeTableHandle.newBuilder()
                .setTableHandle(toProto(handle.getTableHandle()))
                .setInsertHandle(toProto(handle.getInsertHandle()))
                .build();
    }

    private HiveMergeTableHandle fromProto(io.trino.plugin.hive.protobuf.HiveMergeTableHandle proto)
    {
        if (!proto.hasTableHandle() || !proto.hasInsertHandle()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive merge table handle has no table handle or no insert handle");
        }
        return new HiveMergeTableHandle(fromProto(proto.getTableHandle()), fromProto(proto.getInsertHandle()));
    }

    private static io.trino.plugin.hive.protobuf.HiveStorageFormat toProto(HiveStorageFormat format)
    {
        return switch (format) {
            case ORC -> io.trino.plugin.hive.protobuf.HiveStorageFormat.ORC;
            case PARQUET -> io.trino.plugin.hive.protobuf.HiveStorageFormat.PARQUET;
            case AVRO -> io.trino.plugin.hive.protobuf.HiveStorageFormat.AVRO;
            case RCBINARY -> io.trino.plugin.hive.protobuf.HiveStorageFormat.RCBINARY;
            case RCTEXT -> io.trino.plugin.hive.protobuf.HiveStorageFormat.RCTEXT;
            case SEQUENCEFILE -> io.trino.plugin.hive.protobuf.HiveStorageFormat.SEQUENCEFILE;
            case SEQUENCEFILE_PROTOBUF -> io.trino.plugin.hive.protobuf.HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
            case JSON -> io.trino.plugin.hive.protobuf.HiveStorageFormat.JSON;
            case OPENX_JSON -> io.trino.plugin.hive.protobuf.HiveStorageFormat.OPENX_JSON;
            case TEXTFILE -> io.trino.plugin.hive.protobuf.HiveStorageFormat.TEXTFILE;
            case CSV -> io.trino.plugin.hive.protobuf.HiveStorageFormat.CSV;
            case REGEX -> io.trino.plugin.hive.protobuf.HiveStorageFormat.REGEX;
            case ESRI -> io.trino.plugin.hive.protobuf.HiveStorageFormat.ESRI;
            case ESRI_GEO_JSON -> io.trino.plugin.hive.protobuf.HiveStorageFormat.ESRI_GEO_JSON;
        };
    }

    private static HiveStorageFormat fromProto(io.trino.plugin.hive.protobuf.HiveStorageFormat format)
    {
        return switch (format) {
            case ORC -> HiveStorageFormat.ORC;
            case PARQUET -> HiveStorageFormat.PARQUET;
            case AVRO -> HiveStorageFormat.AVRO;
            case RCBINARY -> HiveStorageFormat.RCBINARY;
            case RCTEXT -> HiveStorageFormat.RCTEXT;
            case SEQUENCEFILE -> HiveStorageFormat.SEQUENCEFILE;
            case SEQUENCEFILE_PROTOBUF -> HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
            case JSON -> HiveStorageFormat.JSON;
            case OPENX_JSON -> HiveStorageFormat.OPENX_JSON;
            case TEXTFILE -> HiveStorageFormat.TEXTFILE;
            case CSV -> HiveStorageFormat.CSV;
            case REGEX -> HiveStorageFormat.REGEX;
            case ESRI -> HiveStorageFormat.ESRI;
            case ESRI_GEO_JSON -> HiveStorageFormat.ESRI_GEO_JSON;
            case HIVE_STORAGE_FORMAT_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("storage format", format);
        };
    }

    private static io.trino.plugin.hive.protobuf.LocationHandle toProto(LocationHandle handle)
    {
        return io.trino.plugin.hive.protobuf.LocationHandle.newBuilder()
                .setTargetPath(handle.getJsonSerializableTargetPath())
                .setWritePath(handle.getJsonSerializableWritePath())
                .setWriteMode(switch (handle.getWriteMode()) {
                    case STAGE_AND_MOVE_TO_TARGET_DIRECTORY -> io.trino.plugin.hive.protobuf.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
                    case DIRECT_TO_TARGET_NEW_DIRECTORY -> io.trino.plugin.hive.protobuf.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
                    case DIRECT_TO_TARGET_EXISTING_DIRECTORY -> io.trino.plugin.hive.protobuf.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
                })
                .build();
    }

    private static LocationHandle fromProto(io.trino.plugin.hive.protobuf.LocationHandle proto)
    {
        WriteMode writeMode = switch (proto.getWriteMode()) {
            case STAGE_AND_MOVE_TO_TARGET_DIRECTORY -> WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
            case DIRECT_TO_TARGET_NEW_DIRECTORY -> WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
            case DIRECT_TO_TARGET_EXISTING_DIRECTORY -> WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
            case WRITE_MODE_UNSPECIFIED, UNRECOGNIZED -> throw unknownEnumValue("write mode", proto.getWriteMode());
        };
        return new LocationHandle(proto.getTargetPath(), proto.getWritePath(), writeMode);
    }

    // metastore objects carried by page sink metadata

    private static io.trino.plugin.hive.protobuf.HivePageSinkMetadata toProto(HivePageSinkMetadata metadata)
    {
        io.trino.plugin.hive.protobuf.HivePageSinkMetadata.Builder builder = io.trino.plugin.hive.protobuf.HivePageSinkMetadata.newBuilder()
                .setSchemaName(metadata.getSchemaTableName().getSchemaName())
                .setTableName(metadata.getSchemaTableName().getTableName());
        metadata.getTable().ifPresent(table -> builder.setTable(toProto(table)));
        metadata.getModifiedPartitions().forEach((values, partition) -> {
            ModifiedPartition.Builder entry = ModifiedPartition.newBuilder().addAllPartitionValues(values);
            partition.ifPresent(value -> entry.setPartition(toProto(value)));
            builder.addModifiedPartitions(entry);
        });
        return builder.build();
    }

    private static HivePageSinkMetadata fromProto(io.trino.plugin.hive.protobuf.HivePageSinkMetadata proto)
    {
        ImmutableMap.Builder<List<String>, Optional<Partition>> modifiedPartitions = ImmutableMap.builderWithExpectedSize(proto.getModifiedPartitionsCount());
        for (ModifiedPartition entry : proto.getModifiedPartitionsList()) {
            modifiedPartitions.put(
                    entry.getPartitionValuesList(),
                    entry.hasPartition() ? Optional.of(fromProto(entry.getPartition())) : Optional.empty());
        }
        Map<List<String>, Optional<Partition>> built;
        try {
            built = modifiedPartitions.buildOrThrow();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive page sink metadata repeats a modified partition", e);
        }
        return new HivePageSinkMetadata(
                new SchemaTableName(proto.getSchemaName(), proto.getTableName()),
                proto.hasTable() ? Optional.of(fromProto(proto.getTable())) : Optional.empty(),
                built);
    }

    private static io.trino.plugin.hive.protobuf.Table toProto(Table table)
    {
        io.trino.plugin.hive.protobuf.Table.Builder builder = io.trino.plugin.hive.protobuf.Table.newBuilder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setTableType(table.getTableType())
                .setStorage(toProto(table.getStorage()))
                .putAllParameters(table.getParameters());
        table.getOwner().ifPresent(builder::setOwner);
        table.getDataColumns().forEach(column -> builder.addDataColumns(toProto(column)));
        table.getPartitionColumns().forEach(column -> builder.addPartitionColumns(toProto(column)));
        table.getViewOriginalText().ifPresent(builder::setViewOriginalText);
        table.getViewExpandedText().ifPresent(builder::setViewExpandedText);
        table.getWriteId().ifPresent(builder::setWriteId);
        return builder.build();
    }

    private static Table fromProto(io.trino.plugin.hive.protobuf.Table proto)
    {
        if (!proto.hasStorage()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive table has no storage");
        }
        return new Table(
                proto.getDatabaseName(),
                proto.getTableName(),
                proto.hasOwner() ? Optional.of(proto.getOwner()) : Optional.empty(),
                proto.getTableType(),
                fromProto(proto.getStorage()),
                metastoreColumns(proto.getDataColumnsList()),
                metastoreColumns(proto.getPartitionColumnsList()),
                proto.getParametersMap(),
                proto.hasViewOriginalText() ? Optional.of(proto.getViewOriginalText()) : Optional.empty(),
                proto.hasViewExpandedText() ? Optional.of(proto.getViewExpandedText()) : Optional.empty(),
                proto.hasWriteId() ? OptionalLong.of(proto.getWriteId()) : OptionalLong.empty());
    }

    private static io.trino.plugin.hive.protobuf.Partition toProto(Partition partition)
    {
        io.trino.plugin.hive.protobuf.Partition.Builder builder = io.trino.plugin.hive.protobuf.Partition.newBuilder()
                .setDatabaseName(partition.getDatabaseName())
                .setTableName(partition.getTableName())
                .addAllValues(partition.getValues())
                .setStorage(toProto(partition.getStorage()))
                .putAllParameters(partition.getParameters());
        partition.getColumns().forEach(column -> builder.addColumns(toProto(column)));
        return builder.build();
    }

    private static Partition fromProto(io.trino.plugin.hive.protobuf.Partition proto)
    {
        if (!proto.hasStorage()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive partition has no storage");
        }
        return new Partition(
                proto.getDatabaseName(),
                proto.getTableName(),
                proto.getValuesList(),
                fromProto(proto.getStorage()),
                metastoreColumns(proto.getColumnsList()),
                proto.getParametersMap());
    }

    private static io.trino.plugin.hive.protobuf.Storage toProto(Storage storage)
    {
        io.trino.plugin.hive.protobuf.StorageFormat.Builder format = io.trino.plugin.hive.protobuf.StorageFormat.newBuilder();
        StorageFormat storageFormat = storage.getStorageFormat();
        if (storageFormat.getSerDeNullable() != null) {
            format.setSerde(storageFormat.getSerDeNullable());
        }
        if (storageFormat.getInputFormatNullable() != null) {
            format.setInputFormat(storageFormat.getInputFormatNullable());
        }
        if (storageFormat.getOutputFormatNullable() != null) {
            format.setOutputFormat(storageFormat.getOutputFormatNullable());
        }
        io.trino.plugin.hive.protobuf.Storage.Builder builder = io.trino.plugin.hive.protobuf.Storage.newBuilder()
                .setStorageFormat(format)
                .setSkewed(storage.isSkewed())
                .putAllSerdeParameters(storage.getSerdeParameters());
        storage.getOptionalLocation().ifPresent(builder::setLocation);
        storage.getBucketProperty().ifPresent(property -> {
            io.trino.plugin.hive.protobuf.HiveBucketProperty.Builder propertyBuilder = io.trino.plugin.hive.protobuf.HiveBucketProperty.newBuilder()
                    .addAllBucketedBy(property.bucketedBy())
                    .setBucketCount(property.bucketCount());
            property.sortedBy().forEach(column -> propertyBuilder.addSortedBy(toProto(column)));
            builder.setBucketProperty(propertyBuilder);
        });
        return builder.build();
    }

    private static Storage fromProto(io.trino.plugin.hive.protobuf.Storage proto)
    {
        io.trino.plugin.hive.protobuf.StorageFormat format = proto.getStorageFormat();
        Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        if (proto.hasBucketProperty()) {
            io.trino.plugin.hive.protobuf.HiveBucketProperty property = proto.getBucketProperty();
            bucketProperty = Optional.of(new HiveBucketProperty(
                    property.getBucketedByList(),
                    property.getBucketCount(),
                    sortingColumns(property.getSortedByList())));
        }
        return new Storage(
                StorageFormat.createNullable(
                        format.hasSerde() ? format.getSerde() : null,
                        format.hasInputFormat() ? format.getInputFormat() : null,
                        format.hasOutputFormat() ? format.getOutputFormat() : null),
                proto.hasLocation() ? Optional.of(proto.getLocation()) : Optional.empty(),
                bucketProperty,
                proto.getSkewed(),
                proto.getSerdeParametersMap());
    }

    private static io.trino.plugin.hive.protobuf.Column toProto(Column column)
    {
        io.trino.plugin.hive.protobuf.Column.Builder builder = io.trino.plugin.hive.protobuf.Column.newBuilder()
                .setName(column.getName())
                .setType(column.getType().toString())
                .putAllProperties(column.getProperties());
        column.getComment().ifPresent(builder::setComment);
        return builder.build();
    }

    private static List<Column> metastoreColumns(List<io.trino.plugin.hive.protobuf.Column> protos)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(protos.size());
        for (io.trino.plugin.hive.protobuf.Column proto : protos) {
            columns.add(new Column(
                    proto.getName(),
                    hiveType(proto.getType()),
                    proto.hasComment() ? Optional.of(proto.getComment()) : Optional.empty(),
                    proto.getPropertiesMap()));
        }
        return columns.build();
    }

    // transaction

    private static io.trino.plugin.hive.protobuf.HiveTransactionHandle toProto(HiveTransactionHandle handle)
    {
        return io.trino.plugin.hive.protobuf.HiveTransactionHandle.newBuilder()
                .setAutoCommit(handle.isAutoCommit())
                .setUuid(handle.getUuid().toString())
                .build();
    }

    private static HiveTransactionHandle fromProto(io.trino.plugin.hive.protobuf.HiveTransactionHandle proto)
    {
        UUID uuid;
        try {
            uuid = UUID.fromString(proto.getUuid());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive transaction handle carries an invalid uuid: " + proto.getUuid(), e);
        }
        return new HiveTransactionHandle(proto.getAutoCommit(), uuid);
    }

    // scalar helpers

    private Type type(String typeId)
    {
        try {
            return typeManager.getType(TypeId.of(typeId));
        }
        catch (RuntimeException e) {
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive handle carries an unknown type: " + typeId, e);
        }
    }

    private static HiveType hiveType(String hiveType)
    {
        try {
            return HiveType.valueOf(hiveType);
        }
        catch (RuntimeException e) {
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf hive handle carries an invalid hive type: " + hiveType, e);
        }
    }

    // This codec is a serialization path, the same role the JSON creator plays, and the raw value is
    // the only exact representation: fromProportion rounds up and would change the weight.
    @SuppressWarnings("DoNotCall")
    private static SplitWeight splitWeight(long rawValue)
    {
        return SplitWeight.fromRawValue(rawValue);
    }

    private static TrinoException unknownEnumValue(String field, Enum<?> value)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, format("Protobuf hive handle has an unknown %s: %s", field, value));
    }
}
