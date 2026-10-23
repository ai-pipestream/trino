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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.server.protobuf.ProtoIdentifiers.parseHostAddress;
import static io.trino.server.protobuf.ProtoIdentifiers.parseTransactionId;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Protobuf codecs for the handles of {@link InformationSchemaConnector}.
 */
public final class InformationSchemaHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private static final ConnectorHandleCodec<ConnectorTableHandle> TABLE_HANDLE_CODEC = protobufHandleCodec(
            InformationSchemaTableHandle.class,
            io.trino.connector.v1.InformationSchemaTableHandle.parser(),
            InformationSchemaHandleCodecProvider::toProto,
            InformationSchemaHandleCodecProvider::fromProto);

    private static final ConnectorHandleCodec<ColumnHandle> COLUMN_HANDLE_CODEC = protobufHandleCodec(
            InformationSchemaColumnHandle.class,
            io.trino.connector.v1.InformationSchemaColumnHandle.parser(),
            handle -> io.trino.connector.v1.InformationSchemaColumnHandle.newBuilder()
                    .setColumnName(handle.columnName())
                    .build(),
            proto -> new InformationSchemaColumnHandle(proto.getColumnName()));

    private static final ConnectorHandleCodec<ConnectorSplit> SPLIT_CODEC = protobufHandleCodec(
            InformationSchemaSplit.class,
            io.trino.connector.v1.InformationSchemaSplit.parser(),
            split -> io.trino.connector.v1.InformationSchemaSplit.newBuilder()
                    .setAddress(split.address().toString())
                    .build(),
            proto -> new InformationSchemaSplit(parseHostAddress(proto.getAddress())));

    private static final ConnectorHandleCodec<ConnectorTransactionHandle> TRANSACTION_HANDLE_CODEC = protobufHandleCodec(
            InformationSchemaTransactionHandle.class,
            io.trino.connector.v1.InformationSchemaTransactionHandle.parser(),
            handle -> io.trino.connector.v1.InformationSchemaTransactionHandle.newBuilder()
                    .setTransactionId(handle.transactionId().toString())
                    .build(),
            proto -> new InformationSchemaTransactionHandle(parseTransactionId(proto.getTransactionId())));

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.of(TABLE_HANDLE_CODEC);
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(COLUMN_HANDLE_CODEC);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(SPLIT_CODEC);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(TRANSACTION_HANDLE_CODEC);
    }

    private static io.trino.connector.v1.InformationSchemaTableHandle toProto(InformationSchemaTableHandle handle)
    {
        io.trino.connector.v1.InformationSchemaTableHandle.Builder builder = io.trino.connector.v1.InformationSchemaTableHandle.newBuilder()
                .setCatalogName(handle.catalogName())
                .setTable(toProto(handle.table()));
        for (QualifiedTablePrefix prefix : handle.prefixes()) {
            io.trino.connector.v1.QualifiedTablePrefix.Builder prefixBuilder = io.trino.connector.v1.QualifiedTablePrefix.newBuilder()
                    .setCatalogName(prefix.getCatalogName());
            prefix.getSchemaName().ifPresent(prefixBuilder::setSchemaName);
            prefix.getTableName().ifPresent(prefixBuilder::setTableName);
            builder.addPrefixes(prefixBuilder);
        }
        handle.limit().ifPresent(builder::setLimit);
        return builder.build();
    }

    private static InformationSchemaTableHandle fromProto(io.trino.connector.v1.InformationSchemaTableHandle proto)
    {
        ImmutableSet.Builder<QualifiedTablePrefix> prefixes = ImmutableSet.builderWithExpectedSize(proto.getPrefixesCount());
        for (io.trino.connector.v1.QualifiedTablePrefix prefix : proto.getPrefixesList()) {
            if (prefix.hasTableName() && !prefix.hasSchemaName()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf table prefix has a table name without a schema name: " + prefix.getTableName());
            }
            prefixes.add(new QualifiedTablePrefix(
                    prefix.getCatalogName(),
                    prefix.hasSchemaName() ? Optional.of(prefix.getSchemaName()) : Optional.empty(),
                    prefix.hasTableName() ? Optional.of(prefix.getTableName()) : Optional.empty()));
        }
        return new InformationSchemaTableHandle(
                proto.getCatalogName(),
                fromProto(proto.getTable()),
                prefixes.build(),
                proto.hasLimit() ? OptionalLong.of(proto.getLimit()) : OptionalLong.empty());
    }

    private static InformationSchemaTableName toProto(InformationSchemaTable table)
    {
        return switch (table) {
            case COLUMNS -> InformationSchemaTableName.COLUMNS;
            case TABLES -> InformationSchemaTableName.TABLES;
            case VIEWS -> InformationSchemaTableName.VIEWS;
            case SCHEMATA -> InformationSchemaTableName.SCHEMATA;
            case TABLE_PRIVILEGES -> InformationSchemaTableName.TABLE_PRIVILEGES;
            case ROLES -> InformationSchemaTableName.ROLES;
            case APPLICABLE_ROLES -> InformationSchemaTableName.APPLICABLE_ROLES;
            case ENABLED_ROLES -> InformationSchemaTableName.ENABLED_ROLES;
        };
    }

    private static InformationSchemaTable fromProto(InformationSchemaTableName table)
    {
        return switch (table) {
            case COLUMNS -> InformationSchemaTable.COLUMNS;
            case TABLES -> InformationSchemaTable.TABLES;
            case VIEWS -> InformationSchemaTable.VIEWS;
            case SCHEMATA -> InformationSchemaTable.SCHEMATA;
            case TABLE_PRIVILEGES -> InformationSchemaTable.TABLE_PRIVILEGES;
            case ROLES -> InformationSchemaTable.ROLES;
            case APPLICABLE_ROLES -> InformationSchemaTable.APPLICABLE_ROLES;
            case ENABLED_ROLES -> InformationSchemaTable.ENABLED_ROLES;
            case INFORMATION_SCHEMA_TABLE_NAME_UNSPECIFIED, UNRECOGNIZED -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf information schema table handle has an unknown table: " + table);
        };
    }
}
