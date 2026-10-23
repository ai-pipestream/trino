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
package io.trino.protobuf.handles;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.protobuf.handles.v1.AllOrNoneValueSet;
import io.trino.protobuf.handles.v1.DiscreteSetMarker;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Converts {@link Domain} to and from its protobuf form. The engine uses it
 * for dynamic filter domains and connectors use it for the constraints inside
 * their handles, so every domain on the control plane has one encoding.
 * Values are carried as Trino blocks serialized with the engine's
 * {@link BlockEncodingSerde}, which already is a binary format.
 */
public final class DomainProtoCodec
{
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public DomainProtoCodec(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    public io.trino.protobuf.handles.v1.Domain toProto(Domain domain)
    {
        io.trino.protobuf.handles.v1.Domain.Builder builder = io.trino.protobuf.handles.v1.Domain.newBuilder()
                .setTypeId(domain.getType().getTypeId().getId())
                .setNullAllowed(domain.isNullAllowed());
        switch (domain.getValues()) {
            case SortedRangeSet sortedRangeSet -> {
                io.trino.protobuf.handles.v1.SortedRangeSet.Builder ranges = io.trino.protobuf.handles.v1.SortedRangeSet.newBuilder()
                        .setBounds(serializeBlock(sortedRangeSet.getSortedRanges()))
                        .setDiscreteSetMarker(toProto(sortedRangeSet.getDiscreteSetMarker()));
                for (boolean inclusive : sortedRangeSet.getInclusive()) {
                    ranges.addInclusive(inclusive);
                }
                builder.setSortedRanges(ranges);
            }
            case EquatableValueSet equatableValueSet -> {
                Type type = equatableValueSet.getType();
                Set<EquatableValueSet.ValueEntry> entries = equatableValueSet.getEntries();
                BlockBuilder values = type.createBlockBuilder(null, entries.size());
                for (EquatableValueSet.ValueEntry entry : entries) {
                    writeNativeValue(type, values, entry.getValue());
                }
                builder.setEquatableValues(io.trino.protobuf.handles.v1.EquatableValueSet.newBuilder()
                        .setInclusive(equatableValueSet.inclusive())
                        .setValues(serializeBlock(values.build())));
            }
            default -> builder.setAllOrNone(AllOrNoneValueSet.newBuilder().setAll(domain.getValues().isAll()));
        }
        return builder.build();
    }

    public Domain fromProto(io.trino.protobuf.handles.v1.Domain proto)
    {
        Type type = typeManager.getType(TypeId.of(proto.getTypeId()));
        ValueSet values = switch (proto.getValueSetCase()) {
            case ALL_OR_NONE -> proto.getAllOrNone().getAll() ? ValueSet.all(type) : ValueSet.none(type);
            case SORTED_RANGES -> sortedRangeSet(type, proto.getSortedRanges());
            case EQUATABLE_VALUES -> equatableValueSet(type, proto.getEquatableValues());
            case VALUESET_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf domain of type %s has no value set".formatted(type));
        };
        return Domain.create(values, proto.getNullAllowed());
    }

    /**
     * The {@link TupleDomainKind} to write for a tuple domain. Connectors write
     * the per-column entries themselves, keyed by their own column handle
     * message, and only when the kind is {@link TupleDomainKind#COLUMN_DOMAINS}.
     */
    public static TupleDomainKind kindOf(TupleDomain<?> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return TupleDomainKind.NONE;
        }
        if (tupleDomain.isAll()) {
            return TupleDomainKind.ALL;
        }
        return TupleDomainKind.COLUMN_DOMAINS;
    }

    /**
     * Raises a categorized error for a {@link TupleDomainKind} that cannot be
     * decoded. Callers switch over the kind and route the two sentinel values
     * here.
     */
    public static TrinoException invalidTupleDomainKind(TupleDomainKind kind, String context)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid TupleDomain kind %s in %s".formatted(kind, context));
    }

    private ValueSet sortedRangeSet(Type type, io.trino.protobuf.handles.v1.SortedRangeSet proto)
    {
        Block bounds = deserializeBlock(proto.getBounds());
        int inclusiveCount = proto.getInclusiveCount();
        if (inclusiveCount != bounds.getPositionCount() || inclusiveCount % 2 != 0) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf sorted range set of type %s has %s bounds and %s inclusive flags".formatted(type, bounds.getPositionCount(), inclusiveCount));
        }
        List<Range> ranges = new ArrayList<>(inclusiveCount / 2);
        for (int position = 0; position < inclusiveCount; position += 2) {
            ranges.add(range(type, bounds, position, proto.getInclusive(position), proto.getInclusive(position + 1)));
        }
        if (ranges.isEmpty()) {
            return ValueSet.none(type);
        }
        try {
            // A set the sender knew to be discrete is rebuilt through the discrete value constructors, which
            // produce the same marker and block layout as Domain.singleValue and ValueSet.of. The marker is
            // only a hint: ranges that are not single values are rebuilt as ranges whatever the marker says.
            if (proto.getDiscreteSetMarker() == DiscreteSetMarker.DISCRETE && ranges.stream().allMatch(Range::isSingleValue)) {
                if (ranges.size() == 1) {
                    return ValueSet.of(type, ranges.get(0).getSingleValue());
                }
                return ValueSet.copyOf(type, ranges.stream().map(Range::getSingleValue).toList());
            }
            return SortedRangeSet.copyOf(type, ranges);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf sorted range set of type %s is invalid: %s".formatted(type, e.getMessage()), e);
        }
    }

    private static DiscreteSetMarker toProto(SortedRangeSet.DiscreteSetMarker marker)
    {
        return switch (marker) {
            case UNKNOWN -> DiscreteSetMarker.DISCRETE_SET_MARKER_UNKNOWN;
            case DISCRETE -> DiscreteSetMarker.DISCRETE;
            case NON_DISCRETE -> DiscreteSetMarker.NON_DISCRETE;
        };
    }

    private static Range range(Type type, Block bounds, int lowPosition, boolean lowInclusive, boolean highInclusive)
    {
        boolean lowUnbounded = bounds.isNull(lowPosition);
        boolean highUnbounded = bounds.isNull(lowPosition + 1);
        try {
            if (lowUnbounded && highUnbounded) {
                return Range.all(type);
            }
            Object low = lowUnbounded ? null : readNativeValue(type, bounds, lowPosition);
            Object high = highUnbounded ? null : readNativeValue(type, bounds, lowPosition + 1);
            if (highUnbounded) {
                return lowInclusive ? Range.greaterThanOrEqual(type, low) : Range.greaterThan(type, low);
            }
            if (lowUnbounded) {
                return highInclusive ? Range.lessThanOrEqual(type, high) : Range.lessThan(type, high);
            }
            return Range.range(type, low, lowInclusive, high, highInclusive);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf range %s of type %s is invalid: %s".formatted(lowPosition / 2, type, e.getMessage()), e);
        }
    }

    private ValueSet equatableValueSet(Type type, io.trino.protobuf.handles.v1.EquatableValueSet proto)
    {
        Block values = deserializeBlock(proto.getValues());
        Set<EquatableValueSet.ValueEntry> entries = new LinkedHashSet<>(values.getPositionCount());
        for (int position = 0; position < values.getPositionCount(); position++) {
            if (values.isNull(position)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf equatable value set of type %s has a null value at position %s".formatted(type, position));
            }
            entries.add(EquatableValueSet.ValueEntry.create(type, readNativeValue(type, values, position)));
        }
        try {
            return new EquatableValueSet(type, proto.getInclusive(), entries);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf equatable value set of type %s is invalid: %s".formatted(type, e.getMessage()), e);
        }
    }

    private ByteString serializeBlock(Block block)
    {
        SliceOutput output = new DynamicSliceOutput(toIntExact(blockEncodingSerde.estimatedWriteSize(block)));
        blockEncodingSerde.writeBlock(output, block);
        Slice slice = output.slice();
        return ByteString.copyFrom(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private Block deserializeBlock(ByteString bytes)
    {
        try {
            return blockEncodingSerde.readBlock(Slices.wrappedBuffer(bytes.toByteArray()).getInput());
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode block in protobuf domain", e);
        }
    }
}
