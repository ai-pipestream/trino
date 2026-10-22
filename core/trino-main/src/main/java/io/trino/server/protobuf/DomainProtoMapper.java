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
package io.trino.server.protobuf;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.server.protocol.v1.DiscreteValues;
import io.trino.server.protocol.v1.DomainRange;
import io.trino.server.protocol.v1.DomainValues;
import io.trino.server.protocol.v1.RangeValues;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockSerdeUtil.readBlock;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Maps {@link Domain} to its protobuf form. Scalar values are carried as a
 * single serialized {@link Block} in a {@code bytes} field: the block encoding
 * is binary already, so no JSON is involved.
 */
public class DomainProtoMapper
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final TypeManager typeManager;

    @Inject
    public DomainProtoMapper(BlockEncodingSerde blockEncodingSerde, TypeManager typeManager)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public io.trino.server.protocol.v1.Domain toProto(Domain domain)
    {
        Type type = domain.getType();
        io.trino.server.protocol.v1.Domain.Builder builder = io.trino.server.protocol.v1.Domain.newBuilder()
                .setTypeId(type.getTypeId().getId())
                .setNullAllowed(domain.isNullAllowed());
        domain.getValues().getValuesProcessor().consume(
                ranges -> {
                    List<Object> bounds = new ArrayList<>();
                    RangeValues.Builder rangeValues = RangeValues.newBuilder();
                    for (Range range : ranges.getOrderedRanges()) {
                        rangeValues.addRanges(DomainRange.newBuilder()
                                .setLowInclusive(range.isLowInclusive())
                                .setLowPosition(positionOf(bounds, range.getLowValue().orElse(null)))
                                .setHighInclusive(range.isHighInclusive())
                                .setHighPosition(positionOf(bounds, range.getHighValue().orElse(null)))
                                .build());
                    }
                    rangeValues.setBounds(DomainValues.newBuilder()
                            .setBlock(serializeValues(type, bounds))
                            .build());
                    builder.setRanges(rangeValues.build());
                },
                discreteValues -> builder.setDiscrete(DiscreteValues.newBuilder()
                        .setInclusive(discreteValues.isInclusive())
                        .setValues(DomainValues.newBuilder()
                                .setBlock(serializeValues(type, List.copyOf(discreteValues.getValues())))
                                .build())
                        .build()),
                allOrNone -> builder.setAll(allOrNone.isAll()));
        return builder.build();
    }

    public Domain fromProto(io.trino.server.protocol.v1.Domain proto)
    {
        Type type = typeManager.getType(TypeId.of(proto.getTypeId()));
        ValueSet values = switch (proto.getValuesCase()) {
            case ALL -> proto.getAll() ? ValueSet.all(type) : ValueSet.none(type);
            case DISCRETE -> discreteFromProto(type, proto.getDiscrete());
            case RANGES -> rangesFromProto(type, proto.getRanges());
            case VALUES_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Domain in protobuf dynamic filter domains has no values set");
        };
        return Domain.create(values, proto.getNullAllowed());
    }

    private static int positionOf(List<Object> bounds, Object value)
    {
        if (value == null) {
            return -1;
        }
        bounds.add(value);
        return bounds.size() - 1;
    }

    private ValueSet discreteFromProto(Type type, DiscreteValues discrete)
    {
        List<Object> values = deserializeValues(type, discrete.getValues().getBlock());
        ValueSet valueSet = values.isEmpty() ? ValueSet.none(type) : ValueSet.copyOf(type, values);
        return discrete.getInclusive() ? valueSet : valueSet.complement();
    }

    private ValueSet rangesFromProto(Type type, RangeValues rangeValues)
    {
        List<Object> bounds = deserializeValues(type, rangeValues.getBounds().getBlock());
        List<Range> ranges = rangeValues.getRangesList().stream()
                .map(range -> toRange(type, bounds, range))
                .collect(toImmutableList());
        return ranges.isEmpty() ? ValueSet.none(type) : ValueSet.ofRanges(ranges);
    }

    private static Range toRange(Type type, List<Object> bounds, DomainRange range)
    {
        Object low = range.getLowPosition() < 0 ? null : bounds.get(range.getLowPosition());
        Object high = range.getHighPosition() < 0 ? null : bounds.get(range.getHighPosition());
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

    private ByteString serializeValues(Type type, List<?> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            writeNativeValue(type, blockBuilder, value);
        }
        Block block = blockBuilder.build();
        SliceOutput output = new DynamicSliceOutput(toIntExact(blockEncodingSerde.estimatedWriteSize(block)));
        writeBlock(blockEncodingSerde, output, block);
        Slice slice = output.slice();
        return ByteString.copyFrom(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private List<Object> deserializeValues(Type type, ByteString blockBytes)
    {
        Block block = readBlock(blockEncodingSerde, Slices.wrappedBuffer(blockBytes.toByteArray()));
        List<Object> values = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(readNativeValue(type, block, position));
        }
        return values;
    }
}
