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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf handle codecs for the families tpch implements: table, column,
 * split, partitioning and transaction handles. The pushed-down
 * {@link TupleDomain} constraint in {@link TpchTableHandle} encodes scalar
 * domain values natively; a non-scalar domain value (a block for an array,
 * row or map type) fails loudly instead of falling back to another format.
 */
public class TpchHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final TypeManager typeManager;

    public TpchHandleCodecProvider(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    TpchTableHandle tableHandle = (TpchTableHandle) handle;
                    return io.trino.plugin.tpch.protobuf.TpchTableHandle.newBuilder()
                            .setSchemaName(tableHandle.schemaName())
                            .setTableName(tableHandle.tableName())
                            .setScaleFactor(tableHandle.scaleFactor())
                            .setConstraint(toProto(tableHandle.constraint()))
                            .build();
                },
                proto -> new TpchTableHandle(proto.getSchemaName(), proto.getTableName(), proto.getScaleFactor(), fromProto(proto.getConstraint())),
                io.trino.plugin.tpch.protobuf.TpchTableHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(codec(
                handle -> toProto((TpchColumnHandle) handle),
                this::fromProto,
                io.trino.plugin.tpch.protobuf.TpchColumnHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.of(codec(
                handle -> {
                    TpchSplit split = (TpchSplit) handle;
                    return io.trino.plugin.tpch.protobuf.TpchSplit.newBuilder()
                            .setPartNumber(split.getPartNumber())
                            .setTotalParts(split.getTotalParts())
                            .addAllAddresses(split.getAddresses().stream().map(HostAddress::toString).collect(toImmutableList()))
                            .build();
                },
                proto -> new TpchSplit(
                        proto.getPartNumber(),
                        proto.getTotalParts(),
                        proto.getAddressesList().stream().map(HostAddress::fromString).collect(toImmutableList())),
                io.trino.plugin.tpch.protobuf.TpchSplit.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.of(codec(
                _ -> io.trino.plugin.tpch.protobuf.TpchTransactionHandle.getDefaultInstance(),
                _ -> TpchTransactionHandle.INSTANCE,
                io.trino.plugin.tpch.protobuf.TpchTransactionHandle.parser()));
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorPartitioningHandle>> getPartitioningHandleCodec()
    {
        return Optional.of(codec(
                handle -> {
                    TpchPartitioningHandle partitioningHandle = (TpchPartitioningHandle) handle;
                    return io.trino.plugin.tpch.protobuf.TpchPartitioningHandle.newBuilder()
                            .setTable(partitioningHandle.table())
                            .setTotalRows(partitioningHandle.totalRows())
                            .build();
                },
                proto -> new TpchPartitioningHandle(proto.getTable(), proto.getTotalRows()),
                io.trino.plugin.tpch.protobuf.TpchPartitioningHandle.parser()));
    }

    private io.trino.plugin.tpch.protobuf.TpchColumnHandle toProto(TpchColumnHandle columnHandle)
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

    private io.trino.plugin.tpch.protobuf.TupleDomain toProto(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return io.trino.plugin.tpch.protobuf.TupleDomain.newBuilder()
                    .setKind(io.trino.plugin.tpch.protobuf.TupleDomainKind.ALL)
                    .build();
        }
        if (tupleDomain.isNone()) {
            return io.trino.plugin.tpch.protobuf.TupleDomain.newBuilder()
                    .setKind(io.trino.plugin.tpch.protobuf.TupleDomainKind.NONE)
                    .build();
        }
        io.trino.plugin.tpch.protobuf.TupleDomain.Builder builder = io.trino.plugin.tpch.protobuf.TupleDomain.newBuilder()
                .setKind(io.trino.plugin.tpch.protobuf.TupleDomainKind.ENTRIES);
        tupleDomain.getDomains().orElseThrow().forEach((column, domain) -> builder.addEntries(io.trino.plugin.tpch.protobuf.TupleDomainEntry.newBuilder()
                .setColumn(toProto((TpchColumnHandle) column))
                .setDomain(toProto(domain))
                .build()));
        return builder.build();
    }

    private TupleDomain<ColumnHandle> fromProto(io.trino.plugin.tpch.protobuf.TupleDomain proto)
    {
        return switch (proto.getKind()) {
            case ALL -> TupleDomain.all();
            case NONE -> TupleDomain.none();
            case ENTRIES -> {
                ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
                proto.getEntriesList().forEach(entry -> domains.put(fromProto(entry.getColumn()), fromProto(entry.getDomain())));
                yield TupleDomain.withColumnDomains(domains.buildOrThrow());
            }
            case TUPLE_DOMAIN_KIND_UNSPECIFIED, UNRECOGNIZED -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid TupleDomain kind in tpch table handle");
        };
    }

    private io.trino.plugin.tpch.protobuf.Domain toProto(Domain domain)
    {
        io.trino.plugin.tpch.protobuf.Domain.Builder builder = io.trino.plugin.tpch.protobuf.Domain.newBuilder()
                .setTypeId(domain.getType().getTypeId().getId())
                .setNullAllowed(domain.isNullAllowed());
        domain.getValues().getValuesProcessor().consume(
                ranges -> {
                    io.trino.plugin.tpch.protobuf.DomainRanges.Builder rangesBuilder = io.trino.plugin.tpch.protobuf.DomainRanges.newBuilder();
                    for (Range range : ranges.getOrderedRanges()) {
                        io.trino.plugin.tpch.protobuf.DomainRange.Builder rangeBuilder = io.trino.plugin.tpch.protobuf.DomainRange.newBuilder()
                                .setLowInclusive(range.isLowInclusive())
                                .setHighInclusive(range.isHighInclusive());
                        range.getLowValue().ifPresent(low -> rangeBuilder.setLow(toScalarProto(domain.getType(), low)));
                        range.getHighValue().ifPresent(high -> rangeBuilder.setHigh(toScalarProto(domain.getType(), high)));
                        rangesBuilder.addRanges(rangeBuilder.build());
                    }
                    builder.setRanges(rangesBuilder.build());
                },
                discreteValues -> {
                    io.trino.plugin.tpch.protobuf.DomainDiscreteValues.Builder discreteBuilder = io.trino.plugin.tpch.protobuf.DomainDiscreteValues.newBuilder()
                            .setInclusive(discreteValues.isInclusive());
                    discreteValues.getValues().forEach(value -> discreteBuilder.addValues(toScalarProto(domain.getType(), value)));
                    builder.setDiscrete(discreteBuilder.build());
                },
                allOrNone -> builder.setAll(allOrNone.isAll()));
        return builder.build();
    }

    private Domain fromProto(io.trino.plugin.tpch.protobuf.Domain proto)
    {
        Type type = typeManager.getType(TypeId.of(proto.getTypeId()));
        ValueSet values = switch (proto.getValuesCase()) {
            case ALL -> proto.getAll() ? ValueSet.all(type) : ValueSet.none(type);
            case DISCRETE -> {
                List<Object> discreteValues = proto.getDiscrete().getValuesList().stream()
                        .map(TpchHandleCodecProvider::fromScalarProto)
                        .collect(toImmutableList());
                ValueSet valueSet = discreteValues.isEmpty() ? ValueSet.none(type) : ValueSet.copyOf(type, discreteValues);
                yield proto.getDiscrete().getInclusive() ? valueSet : valueSet.complement();
            }
            case RANGES -> {
                List<Range> ranges = proto.getRanges().getRangesList().stream()
                        .map(range -> toRange(type, range))
                        .collect(toImmutableList());
                yield ranges.isEmpty() ? ValueSet.none(type) : ValueSet.ofRanges(ranges);
            }
            case VALUES_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Domain in tpch table handle has no values set");
        };
        return Domain.create(values, proto.getNullAllowed());
    }

    private static Range toRange(Type type, io.trino.plugin.tpch.protobuf.DomainRange range)
    {
        Object low = range.hasLow() ? fromScalarProto(range.getLow()) : null;
        Object high = range.hasHigh() ? fromScalarProto(range.getHigh()) : null;
        if (low == null && high == null) {
            return Range.all(type);
        }
        if (high == null) {
            return range.getLowInclusive() ? Range.greaterThanOrEqual(type, low) : Range.greaterThan(type, low);
        }
        if (low == null) {
            return range.getHighInclusive() ? Range.lessThanOrEqual(type, high) : Range.lessThan(type, high);
        }
        return Range.range(type, low, range.getLowInclusive(), high, range.getHighInclusive());
    }

    private static io.trino.plugin.tpch.protobuf.ScalarValue toScalarProto(Type type, Object value)
    {
        io.trino.plugin.tpch.protobuf.ScalarValue.Builder builder = io.trino.plugin.tpch.protobuf.ScalarValue.newBuilder();
        switch (value) {
            case Long longValue -> builder.setLongValue(longValue);
            case Integer intValue -> builder.setLongValue(intValue.longValue());
            case Double doubleValue -> builder.setDoubleValue(doubleValue);
            case Boolean boolValue -> builder.setBoolValue(boolValue);
            case Slice sliceValue -> builder.setSliceValue(ByteString.copyFrom(sliceValue.byteArray(), sliceValue.byteArrayOffset(), sliceValue.length()));
            case Int128 int128Value -> builder.setInt128Value(io.trino.plugin.tpch.protobuf.Int128Value.newBuilder()
                    .setHigh(int128Value.getHigh())
                    .setLow(int128Value.getLow())
                    .build());
            default -> throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Tpch protobuf handle codec does not support non-scalar domain values of type %s (value class %s)", type, value.getClass().getName()));
        }
        return builder.build();
    }

    private static Object fromScalarProto(io.trino.plugin.tpch.protobuf.ScalarValue proto)
    {
        return switch (proto.getValueCase()) {
            case LONG_VALUE -> proto.getLongValue();
            case DOUBLE_VALUE -> proto.getDoubleValue();
            case BOOL_VALUE -> proto.getBoolValue();
            case SLICE_VALUE -> Slices.wrappedBuffer(proto.getSliceValue().toByteArray());
            case INT128_VALUE -> Int128.valueOf(proto.getInt128Value().getHigh(), proto.getInt128Value().getLow());
            case VALUE_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Scalar value in tpch table handle has no value set");
        };
    }

    private static <H, P extends Message> ConnectorHandleCodec<H> codec(Function<H, P> toProto, Function<P, H> fromProto, Parser<P> parser)
    {
        return new ConnectorHandleCodec<>()
        {
            @Override
            public byte[] serialize(H handle)
            {
                return toProto.apply(handle).toByteArray();
            }

            @Override
            public H deserialize(byte[] bytes)
            {
                try {
                    return fromProto.apply(parser.parseFrom(bytes));
                }
                catch (InvalidProtocolBufferException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf tpch handle", e);
                }
            }
        };
    }
}
