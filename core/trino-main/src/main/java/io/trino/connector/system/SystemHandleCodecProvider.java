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
import io.trino.connector.CatalogHandle;
import io.trino.connector.v1.SystemColumnDomain;
import io.trino.connector.v1.SystemTupleDomain;
import io.trino.protobuf.handles.DomainProtoCodec;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.server.protocol.v1.ConnectorHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;

import static com.google.protobuf.ByteString.copyFrom;
import static io.trino.metadata.ConnectorHandleProtobufCodec.missingCodec;
import static io.trino.metadata.HandleFamily.TRANSACTION;
import static io.trino.protobuf.handles.DomainProtoCodec.invalidTupleDomainKind;
import static io.trino.protobuf.handles.DomainProtoCodec.kindOf;
import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.server.protobuf.ProtoIdentifiers.parseCatalogHandle;
import static io.trino.server.protobuf.ProtoIdentifiers.parseHostAddress;
import static io.trino.server.protobuf.ProtoIdentifiers.parseTransactionId;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf codecs for the handles of a catalog's {@link SystemConnector}.
 * The transaction handle nests the served catalog's own transaction handle,
 * which travels as that catalog's protobuf payload; a catalog without a
 * transaction handle codec therefore cannot ship system table handles either.
 */
public final class SystemHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final ConnectorHandleCodec<ConnectorTableHandle> tableHandleCodec;
    private final ConnectorHandleCodec<ColumnHandle> columnHandleCodec;
    private final ConnectorHandleCodec<ConnectorSplit> splitCodec;
    private final ConnectorHandleCodec<ConnectorTransactionHandle> transactionHandleCodec;

    public SystemHandleCodecProvider(
            CatalogHandle catalogHandle,
            Optional<ConnectorHandleCodecProvider> catalogCodecProvider,
            DomainProtoCodec domainProtoCodec)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(catalogCodecProvider, "catalogCodecProvider is null");
        requireNonNull(domainProtoCodec, "domainProtoCodec is null");

        tableHandleCodec = protobufHandleCodec(
                SystemTableHandle.class,
                io.trino.connector.v1.SystemTableHandle.parser(),
                handle -> io.trino.connector.v1.SystemTableHandle.newBuilder()
                        .setSchemaName(handle.schemaName())
                        .setTableName(handle.tableName())
                        .setConstraint(toProto(domainProtoCodec, handle.constraint()))
                        .build(),
                proto -> new SystemTableHandle(
                        proto.getSchemaName(),
                        proto.getTableName(),
                        fromProto(domainProtoCodec, proto.getConstraint())));

        columnHandleCodec = protobufHandleCodec(
                SystemColumnHandle.class,
                io.trino.connector.v1.SystemColumnHandle.parser(),
                handle -> io.trino.connector.v1.SystemColumnHandle.newBuilder()
                        .setColumnName(handle.columnName())
                        .build(),
                proto -> new SystemColumnHandle(proto.getColumnName()));

        splitCodec = protobufHandleCodec(
                SystemSplit.class,
                io.trino.connector.v1.SystemSplit.parser(),
                split -> {
                    io.trino.connector.v1.SystemSplit.Builder builder = io.trino.connector.v1.SystemSplit.newBuilder()
                            .setConstraint(toProto(domainProtoCodec, split.getConstraint()));
                    for (HostAddress address : split.getAddresses()) {
                        builder.addAddresses(address.toString());
                    }
                    split.getCatalogName().ifPresent(builder::setCatalogName);
                    return builder.build();
                },
                proto -> {
                    if (proto.getAddressesCount() == 0) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf system split has no addresses");
                    }
                    ImmutableList.Builder<HostAddress> addresses = ImmutableList.builderWithExpectedSize(proto.getAddressesCount());
                    for (String address : proto.getAddressesList()) {
                        addresses.add(parseHostAddress(address));
                    }
                    return new SystemSplit(
                            addresses.build(),
                            fromProto(domainProtoCodec, proto.getConstraint()),
                            proto.hasCatalogName() ? Optional.of(proto.getCatalogName()) : Optional.empty());
                });

        transactionHandleCodec = protobufHandleCodec(
                SystemTransactionHandle.class,
                io.trino.connector.v1.SystemTransactionHandle.parser(),
                handle -> {
                    ConnectorTransactionHandle nested = handle.getConnectorTransactionHandle();
                    ConnectorHandleCodec<ConnectorTransactionHandle> nestedCodec = catalogCodecProvider.flatMap(TRANSACTION::codecFrom)
                            .orElseThrow(() -> missingCodec(catalogHandle, TRANSACTION, nested.getClass()));
                    return io.trino.connector.v1.SystemTransactionHandle.newBuilder()
                            .setTransactionId(handle.getTransactionId().toString())
                            .setConnectorTransactionHandle(ConnectorHandle.newBuilder()
                                    .setCatalogHandle(catalogHandle.getId())
                                    .setPayload(copyFrom(nestedCodec.serialize(nested))))
                            .build();
                },
                proto -> {
                    if (!proto.hasConnectorTransactionHandle()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf system transaction handle has no connector transaction handle");
                    }
                    ConnectorHandle nested = proto.getConnectorTransactionHandle();
                    CatalogHandle nestedCatalog = parseCatalogHandle(nested.getCatalogHandle());
                    if (!nestedCatalog.equals(catalogHandle)) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                                "Protobuf system transaction handle of catalog '%s' nests a transaction handle of catalog '%s'",
                                catalogHandle.getCatalogName(),
                                nestedCatalog.getCatalogName()));
                    }
                    ConnectorHandleCodec<ConnectorTransactionHandle> nestedCodec = catalogCodecProvider.flatMap(TRANSACTION::codecFrom)
                            .orElseThrow(() -> missingCodec(catalogHandle, TRANSACTION, ConnectorTransactionHandle.class));
                    ConnectorTransactionHandle nestedHandle = nestedCodec.deserialize(nested.getPayload().toByteArray());
                    if (nestedHandle == null) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Protobuf handle codec of catalog '%s' decoded a transaction handle as null", catalogHandle.getCatalogName()));
                    }
                    return new SystemTransactionHandle(parseTransactionId(proto.getTransactionId()), nestedHandle);
                });
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

    static SystemTupleDomain toProto(DomainProtoCodec domainProtoCodec, TupleDomain<ColumnHandle> tupleDomain)
    {
        SystemTupleDomain.Builder builder = SystemTupleDomain.newBuilder()
                .setKind(kindOf(tupleDomain));
        if (tupleDomain.getDomains().isPresent()) {
            tupleDomain.getDomains().get().forEach((column, domain) -> {
                if (!(column instanceof SystemColumnHandle systemColumn)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "System table constraint is keyed by " + column.getClass().getName() + " instead of " + SystemColumnHandle.class.getName());
                }
                builder.addColumnDomains(SystemColumnDomain.newBuilder()
                        .setColumnName(systemColumn.columnName())
                        .setDomain(domainProtoCodec.toProto(domain)));
            });
        }
        return builder.build();
    }

    static TupleDomain<ColumnHandle> fromProto(DomainProtoCodec domainProtoCodec, SystemTupleDomain proto)
    {
        TupleDomainKind kind = proto.getKind();
        return switch (kind) {
            case ALL -> TupleDomain.all();
            case NONE -> TupleDomain.none();
            case COLUMN_DOMAINS -> {
                ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builderWithExpectedSize(proto.getColumnDomainsCount());
                for (SystemColumnDomain columnDomain : proto.getColumnDomainsList()) {
                    if (!columnDomain.hasDomain()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf system table constraint has no domain for column " + columnDomain.getColumnName());
                    }
                    domains.put(new SystemColumnHandle(columnDomain.getColumnName()), domainProtoCodec.fromProto(columnDomain.getDomain()));
                }
                Map<ColumnHandle, Domain> built;
                try {
                    built = domains.buildOrThrow();
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf system table constraint repeats a column", e);
                }
                yield TupleDomain.withColumnDomains(built);
            }
            case TUPLE_DOMAIN_KIND_UNSPECIFIED, UNRECOGNIZED -> throw invalidTupleDomainKind(kind, "system table constraint");
        };
    }
}
