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

import com.google.inject.Inject;
import io.trino.plugin.tpch.protobuf.TpchColumnDomain;
import io.trino.plugin.tpch.protobuf.TpchTupleDomain;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.protobuf.handles.DomainProtoCodec.invalidTupleDomainKind;
import static io.trino.protobuf.handles.DomainProtoCodec.kindOf;
import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf handle codecs for the families tpch implements: table, column,
 * split, partitioning and transaction handles. The pushed-down
 * {@link TupleDomain} constraint in {@link TpchTableHandle} uses the shared
 * domain encoding, so a value of any column type travels as a Trino block.
 */
public class TpchHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final TypeManager typeManager;
    private final DomainProtoCodec domainProtoCodec;

    private final ConnectorHandleCodec<ConnectorTableHandle> tableHandleCodec;
    private final ConnectorHandleCodec<ColumnHandle> columnHandleCodec;
    private final ConnectorHandleCodec<ConnectorSplit> splitCodec;
    private final ConnectorHandleCodec<ConnectorTransactionHandle> transactionHandleCodec;
    private final ConnectorHandleCodec<ConnectorPartitioningHandle> partitioningHandleCodec;

    @Inject
    public TpchHandleCodecProvider(TypeManager typeManager, DomainProtoCodec domainProtoCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.domainProtoCodec = requireNonNull(domainProtoCodec, "domainProtoCodec is null");

        tableHandleCodec = protobufHandleCodec(
                TpchTableHandle.class,
                io.trino.plugin.tpch.protobuf.TpchTableHandle.parser(),
                handle -> io.trino.plugin.tpch.protobuf.TpchTableHandle.newBuilder()
                        .setSchemaName(handle.schemaName())
                        .setTableName(handle.tableName())
                        .setScaleFactor(handle.scaleFactor())
                        .setConstraint(toProto(handle.constraint()))
                        .build(),
                proto -> new TpchTableHandle(proto.getSchemaName(), proto.getTableName(), proto.getScaleFactor(), fromProto(proto.getConstraint())));

        columnHandleCodec = protobufHandleCodec(
                TpchColumnHandle.class,
                io.trino.plugin.tpch.protobuf.TpchColumnHandle.parser(),
                TpchHandleCodecProvider::toProto,
                this::fromProto);

        splitCodec = protobufHandleCodec(
                TpchSplit.class,
                io.trino.plugin.tpch.protobuf.TpchSplit.parser(),
                split -> io.trino.plugin.tpch.protobuf.TpchSplit.newBuilder()
                        .setPartNumber(split.getPartNumber())
                        .setTotalParts(split.getTotalParts())
                        .addAllAddresses(split.getAddresses().stream().map(HostAddress::toString).collect(toImmutableList()))
                        .build(),
                proto -> new TpchSplit(
                        proto.getPartNumber(),
                        proto.getTotalParts(),
                        proto.getAddressesList().stream().map(HostAddress::fromString).collect(toImmutableList())));

        transactionHandleCodec = protobufHandleCodec(
                TpchTransactionHandle.class,
                io.trino.plugin.tpch.protobuf.TpchTransactionHandle.parser(),
                _ -> io.trino.plugin.tpch.protobuf.TpchTransactionHandle.getDefaultInstance(),
                _ -> TpchTransactionHandle.INSTANCE);

        partitioningHandleCodec = protobufHandleCodec(
                TpchPartitioningHandle.class,
                io.trino.plugin.tpch.protobuf.TpchPartitioningHandle.parser(),
                handle -> io.trino.plugin.tpch.protobuf.TpchPartitioningHandle.newBuilder()
                        .setTable(handle.table())
                        .setTotalRows(handle.totalRows())
                        .build(),
                proto -> new TpchPartitioningHandle(proto.getTable(), proto.getTotalRows()));
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
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(transactionHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorPartitioningHandle>> getPartitioningHandleCodec()
    {
        return Optional.of(partitioningHandleCodec);
    }

    private static io.trino.plugin.tpch.protobuf.TpchColumnHandle toProto(TpchColumnHandle columnHandle)
    {
        return io.trino.plugin.tpch.protobuf.TpchColumnHandle.newBuilder()
                .setColumnName(columnHandle.columnName())
                .setTypeId(columnHandle.type().getTypeId().getId())
                .build();
    }

    private TpchColumnHandle fromProto(io.trino.plugin.tpch.protobuf.TpchColumnHandle proto)
    {
        return new TpchColumnHandle(proto.getColumnName(), typeManager.getType(TypeId.of(proto.getTypeId())));
    }

    private TpchTupleDomain toProto(TupleDomain<ColumnHandle> constraint)
    {
        TpchTupleDomain.Builder builder = TpchTupleDomain.newBuilder()
                .setKind(kindOf(constraint));
        if (!constraint.isAll() && !constraint.isNone()) {
            constraint.getDomains().orElseThrow().forEach((column, domain) -> {
                if (!(column instanceof TpchColumnHandle tpchColumn)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Tpch table constraint is keyed by %s instead of %s".formatted(column.getClass().getName(), TpchColumnHandle.class.getName()));
                }
                builder.addColumnDomains(TpchColumnDomain.newBuilder()
                        .setColumn(toProto(tpchColumn))
                        .setDomain(domainProtoCodec.toProto(domain)));
            });
        }
        return builder.build();
    }

    private TupleDomain<ColumnHandle> fromProto(TpchTupleDomain proto)
    {
        return switch (proto.getKind()) {
            case ALL -> TupleDomain.all();
            case NONE -> TupleDomain.none();
            case COLUMN_DOMAINS -> {
                Map<ColumnHandle, Domain> domains = new LinkedHashMap<>();
                for (TpchColumnDomain entry : proto.getColumnDomainsList()) {
                    if (!entry.hasColumn() || !entry.hasDomain()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf tpch table constraint has an entry without a column or a domain");
                    }
                    TpchColumnHandle column = fromProto(entry.getColumn());
                    if (domains.put(column, domainProtoCodec.fromProto(entry.getDomain())) != null) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf tpch table constraint repeats column " + column.columnName());
                    }
                }
                yield TupleDomain.withColumnDomains(domains);
            }
            case TUPLE_DOMAIN_KIND_UNSPECIFIED, UNRECOGNIZED -> throw invalidTupleDomainKind(proto.getKind(), "tpch table constraint");
        };
    }
}
