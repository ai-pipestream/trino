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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slices;
import io.trino.protobuf.handles.v1.AllOrNoneValueSet;
import io.trino.protobuf.handles.v1.DiscreteSetMarker;
import io.trino.protobuf.handles.v1.TupleDomainKind;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.protobuf.handles.TestingTypeManager.BIGINT_MAP;
import static io.trino.protobuf.handles.TestingTypeManager.LONG_DECIMAL;
import static io.trino.protobuf.handles.TestingTypeManager.SHORT_DECIMAL;
import static io.trino.protobuf.handles.TestingTypeManager.VARCHAR_10;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.predicate.SortedRangeSet.DiscreteSetMarker.DISCRETE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDomainProtoCodec
{
    private static final TestingTypeManager TYPE_MANAGER = new TestingTypeManager();
    private static final TestingBlockEncodingSerde SERDE = new TestingBlockEncodingSerde(TYPE_MANAGER);
    private final DomainProtoCodec codec = new DomainProtoCodec(TYPE_MANAGER, SERDE);

    @Test
    public void testAllAndNone()
    {
        for (Type type : List.of(BIGINT, VARCHAR, BIGINT_MAP, LONG_DECIMAL)) {
            assertRoundTrip(Domain.all(type));
            assertRoundTrip(Domain.none(type));
            assertRoundTrip(Domain.onlyNull(type));
            assertRoundTrip(Domain.notNull(type));
        }
    }

    @Test
    public void testSortedRanges()
    {
        assertRoundTrip(Domain.singleValue(BIGINT, 42L));
        assertRoundTrip(Domain.singleValue(BIGINT, Long.MIN_VALUE, true));
        assertRoundTrip(Domain.multipleValues(INTEGER, List.of(-1L, 0L, (long) Integer.MAX_VALUE)));
        assertRoundTrip(Domain.multipleValues(VARCHAR, List.of(utf8Slice(""), utf8Slice("apple"), utf8Slice("é中😀"))));
        assertRoundTrip(Domain.multipleValues(VARCHAR_10, List.of(utf8Slice("a"), utf8Slice("abcdefghij"))));
        assertRoundTrip(Domain.multipleValues(VARBINARY, List.of(Slices.wrappedBuffer(new byte[] {0, -1, 127}), Slices.EMPTY_SLICE)));
        assertRoundTrip(Domain.multipleValues(BOOLEAN, List.of(true, false)));
        assertRoundTrip(Domain.multipleValues(DATE, List.of(-719528L, 0L, 2932896L)));
        assertRoundTrip(Domain.multipleValues(SHORT_DECIMAL, List.of(-12345L, 99999999999L)));
        assertRoundTrip(Domain.multipleValues(LONG_DECIMAL, List.of(Int128.valueOf(-1L, 2L), Int128.valueOf(Long.MAX_VALUE, Long.MIN_VALUE))));
        assertRoundTrip(Domain.multipleValues(TIMESTAMP_MILLIS, List.of(-1_000_000L, 1_700_000_000_000_000L)));
        assertRoundTrip(Domain.multipleValues(TIMESTAMP_NANOS, List.of(new LongTimestamp(1_700_000_000_000_000L, 999_000), new LongTimestamp(-1L, 1))));
        assertRoundTrip(Domain.multipleValues(REAL, List.of((long) floatToRawIntBits(1.5f), (long) floatToRawIntBits(-0.0f))));
        assertRoundTrip(Domain.create(ValueSet.ofRanges(
                Range.lessThan(DOUBLE, -2.5),
                Range.range(DOUBLE, 0.0, false, 1.0, true),
                Range.greaterThanOrEqual(DOUBLE, 1e300)), true));
        assertRoundTrip(Domain.create(ValueSet.ofRanges(Range.all(BIGINT)), false));
        assertRoundTrip(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 7L)), false));
        assertRoundTrip(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("m"))), true));
        assertRoundTrip(Domain.multipleValues(BIGINT, LongStream.range(0, 10_000).boxed().toList()));
    }

    @Test
    public void testEquatableValues()
    {
        assertRoundTrip(Domain.singleValue(BIGINT_MAP, mapOf(1, 2)));
        assertRoundTrip(Domain.multipleValues(BIGINT_MAP, List.of(mapOf(1, 2), mapOf(3, 4), mapOf())));
        assertRoundTrip(Domain.create(ValueSet.of(BIGINT_MAP, mapOf(5, 6)).complement(), true));
        assertRoundTrip(Domain.create(ValueSet.none(BIGINT_MAP), false));
    }

    @Test
    public void testDiscreteSetMarkerIsOnlyAHint()
    {
        Domain ranges = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 10L, false), Range.greaterThan(BIGINT, 20L)), false);
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(ranges);
        assertThat(proto.getSortedRanges().getDiscreteSetMarker()).isEqualTo(DiscreteSetMarker.DISCRETE_SET_MARKER_UNKNOWN);

        // a sender claiming that real ranges are a discrete set does not make them one
        io.trino.protobuf.handles.v1.Domain lying = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder().setDiscreteSetMarker(DiscreteSetMarker.DISCRETE))
                .build();
        Domain decoded = codec.fromProto(lying);
        assertThat(decoded).isEqualTo(ranges);
        assertThat(decoded.getValues().isDiscreteSet()).isFalse();

        // a set the sender had not classified is rebuilt as ranges and classified on demand
        Domain values = Domain.multipleValues(BIGINT, List.of(1L, 2L));
        io.trino.protobuf.handles.v1.Domain unmarked = codec.toProto(values);
        assertThat(unmarked.getSortedRanges().getDiscreteSetMarker()).isEqualTo(DiscreteSetMarker.DISCRETE);
        unmarked = unmarked.toBuilder()
                .setSortedRanges(unmarked.getSortedRanges().toBuilder().setDiscreteSetMarker(DiscreteSetMarker.DISCRETE_SET_MARKER_UNKNOWN))
                .build();
        decoded = codec.fromProto(unmarked);
        assertThat(decoded).isEqualTo(values);
        assertThat(decoded.getValues().isDiscreteSet()).isTrue();
        assertThat(((SortedRangeSet) decoded.getValues()).getDiscreteSetMarker()).isEqualTo(DISCRETE);
    }

    @Test
    public void testDuplicateDiscreteValuesCollapse()
    {
        Domain values = Domain.multipleValues(BIGINT, List.of(3L, 1L, 2L));
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(values);
        // encode the value 2 twice as two single-value ranges
        io.trino.protobuf.handles.v1.SortedRangeSet set = proto.getSortedRanges();
        BlockBuilder bounds = BIGINT.createBlockBuilder(null, 8);
        for (long value : new long[] {1, 1, 2, 2, 2, 2, 3, 3}) {
            BIGINT.writeLong(bounds, value);
        }
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        SERDE.writeBlock(output, bounds.build());
        io.trino.protobuf.handles.v1.Domain duplicated = proto.toBuilder()
                .setSortedRanges(set.toBuilder()
                        .setBounds(ByteString.copyFrom(output.slice().getBytes()))
                        .clearInclusive()
                        .addAllInclusive(List.of(true, true, true, true, true, true, true, true)))
                .build();
        assertThat(codec.fromProto(duplicated)).isEqualTo(values);
    }

    @Test
    public void testUnorderedDiscreteValuesAreSorted()
    {
        Domain values = Domain.multipleValues(BIGINT, List.of(1L, 3L));
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(values);
        io.trino.protobuf.handles.v1.Domain unordered = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder()
                        .setBounds(serialize(BIGINT, 3L, 3L, 1L, 1L)))
                .build();
        Domain decoded = codec.fromProto(unordered);
        assertThat(decoded).isEqualTo(values);
        assertThat(((SortedRangeSet) decoded.getValues()).getDiscreteSetMarker()).isEqualTo(DISCRETE);
    }

    @Test
    public void testNaNInDiscreteSetIsRejected()
    {
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(DOUBLE, 1.0));
        assertThat(proto.getSortedRanges().getDiscreteSetMarker()).isEqualTo(DiscreteSetMarker.DISCRETE);
        io.trino.protobuf.handles.v1.Domain nan = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder()
                        .setBounds(serialize(DOUBLE, Double.NaN, Double.NaN)))
                .build();
        assertThatThrownBy(() -> codec.fromProto(nan))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining("NaN");
    }

    @Test
    public void testSortedRangesWireShape()
    {
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 1L),
                Range.range(BIGINT, 5L, true, 10L, false)), true));
        assertThat(proto.getTypeId()).isEqualTo("bigint");
        assertThat(proto.getNullAllowed()).isTrue();
        assertThat(proto.hasSortedRanges()).isTrue();
        // two ranges, so two positions each: (null, 1), (5, 10)
        assertThat(proto.getSortedRanges().getInclusiveList()).containsExactly(false, false, true, false);
    }

    @Test
    public void testMismatchedInclusiveFlagsAreRejected()
    {
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT, 1L));
        io.trino.protobuf.handles.v1.Domain mismatched = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder().addInclusive(true))
                .build();
        assertThatThrownBy(() -> codec.fromProto(mismatched))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("2 bounds and 3 inclusive flags");
    }

    @Test
    public void testCorruptBlockIsRejected()
    {
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT, 1L));
        io.trino.protobuf.handles.v1.Domain corrupt = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder().setBounds(ByteString.copyFrom(new byte[] {1, 2, 3})))
                .build();
        assertThatThrownBy(() -> codec.fromProto(corrupt))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining("Failed to decode block");
    }

    @Test
    public void testMissingValueSetIsRejected()
    {
        io.trino.protobuf.handles.v1.Domain missing = io.trino.protobuf.handles.v1.Domain.newBuilder()
                .setTypeId("bigint")
                .build();
        assertThatThrownBy(() -> codec.fromProto(missing))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("no value set");
    }

    @Test
    public void testUnknownTypeIsRejected()
    {
        io.trino.protobuf.handles.v1.Domain unknown = io.trino.protobuf.handles.v1.Domain.newBuilder()
                .setTypeId("no_such_type")
                .setAllOrNone(AllOrNoneValueSet.newBuilder().setAll(true))
                .build();
        assertThatThrownBy(() -> codec.fromProto(unknown))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("no_such_type");
    }

    @Test
    public void testBlockOfAnotherTypeIsRejected()
    {
        // the bounds of a bigint domain encoded as a varchar block
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT, 1L));
        io.trino.protobuf.handles.v1.Domain varcharBounds = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder()
                        .setBounds(serialize(VARCHAR, utf8Slice("a"), utf8Slice("a"))))
                .build();
        assertThatThrownBy(() -> codec.fromProto(varcharBounds))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining("of type bigint carries a VariableWidthBlock instead of a LongArrayBlock");

        io.trino.protobuf.handles.v1.Domain equatable = codec.toProto(Domain.singleValue(BIGINT_MAP, mapOf(1, 2)));
        io.trino.protobuf.handles.v1.Domain bigintValues = equatable.toBuilder()
                .setEquatableValues(equatable.getEquatableValues().toBuilder()
                        .setValues(serialize(BIGINT, 1L)))
                .build();
        assertThatThrownBy(() -> codec.fromProto(bigintValues))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("carries a LongArrayBlock instead of a MapBlock");
    }

    @Test
    public void testNestedBlockOfAnotherTypeIsRejected()
    {
        // a map block whose keys and values are varchar where the domain type says bigint: the outer
        // block class matches, so the mismatch surfaces while the nested values are read
        Type varcharMap = new MapType(VARCHAR, VARCHAR, new TypeOperators());
        BlockBuilder builder = varcharMap.createBlockBuilder(null, 1);
        ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeSlice(keyBuilder, utf8Slice("k"));
            VARCHAR.writeSlice(valueBuilder, utf8Slice("v"));
        });
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        SERDE.writeBlock(output, builder.build());
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT_MAP, mapOf(1, 2)));
        io.trino.protobuf.handles.v1.Domain nestedMismatch = proto.toBuilder()
                .setEquatableValues(proto.getEquatableValues().toBuilder()
                        .setValues(ByteString.copyFrom(output.slice().getBytes())))
                .build();
        assertThatThrownBy(() -> codec.fromProto(nestedMismatch))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining("domain of type map(bigint, bigint)");
    }

    @Test
    public void testTrailingBytesAfterBlockAreRejected()
    {
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT, 1L));
        io.trino.protobuf.handles.v1.Domain trailing = proto.toBuilder()
                .setSortedRanges(proto.getSortedRanges().toBuilder()
                        .setBounds(proto.getSortedRanges().getBounds().concat(ByteString.copyFrom(new byte[] {0, 0, 0}))))
                .build();
        assertThatThrownBy(() -> codec.fromProto(trailing))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("has 3 bytes after its block");
    }

    @Test
    public void testInvalidTypeIdIsCategorized()
    {
        for (String typeId : List.of("", "varchar(-1)", "map(bigint")) {
            io.trino.protobuf.handles.v1.Domain invalid = io.trino.protobuf.handles.v1.Domain.newBuilder()
                    .setTypeId(typeId)
                    .setAllOrNone(AllOrNoneValueSet.newBuilder().setAll(true))
                    .build();
            assertThatThrownBy(() -> codec.fromProto(invalid))
                    .isInstanceOf(TrinoException.class)
                    .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
        }
    }

    @Test
    public void testNullInEquatableValuesIsRejected()
    {
        BlockBuilder values = BIGINT_MAP.createBlockBuilder(null, 1);
        values.appendNull();
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        SERDE.writeBlock(output, values.build());
        io.trino.protobuf.handles.v1.Domain proto = codec.toProto(Domain.singleValue(BIGINT_MAP, mapOf(1, 2)));
        io.trino.protobuf.handles.v1.Domain withNull = proto.toBuilder()
                .setEquatableValues(proto.getEquatableValues().toBuilder()
                        .setValues(ByteString.copyFrom(output.slice().getBytes())))
                .build();
        assertThatThrownBy(() -> codec.fromProto(withNull))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("null value");
    }

    @Test
    public void testTupleDomainKind()
    {
        assertThat(DomainProtoCodec.kindOf(TupleDomain.all())).isEqualTo(TupleDomainKind.ALL);
        assertThat(DomainProtoCodec.kindOf(TupleDomain.none())).isEqualTo(TupleDomainKind.NONE);
        assertThat(DomainProtoCodec.kindOf(TupleDomain.withColumnDomains(Map.of("c", Domain.singleValue(BIGINT, 1L))))).isEqualTo(TupleDomainKind.COLUMN_DOMAINS);
        // an entry whose domain is none collapses the whole tuple domain to none
        assertThat(DomainProtoCodec.kindOf(TupleDomain.withColumnDomains(Map.of("c", Domain.none(BIGINT))))).isEqualTo(TupleDomainKind.NONE);
        assertThat(DomainProtoCodec.invalidTupleDomainKind(TupleDomainKind.UNRECOGNIZED, "test"))
                .hasMessageContaining("UNRECOGNIZED")
                .hasMessageContaining("test");
    }

    private void assertRoundTrip(Domain domain)
    {
        io.trino.protobuf.handles.v1.Domain reparsed;
        try {
            reparsed = io.trino.protobuf.handles.v1.Domain.parseFrom(codec.toProto(domain).toByteString());
        }
        catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
        Domain decoded = codec.fromProto(reparsed);
        assertThat(decoded).isEqualTo(domain);
        assertThat(decoded.getValues().getClass()).isEqualTo(domain.getValues().getClass());
        if (domain.getValues() instanceof SortedRangeSet ranges) {
            SortedRangeSet decodedRanges = (SortedRangeSet) decoded.getValues();
            assertThat(decodedRanges.getOrderedRanges()).isEqualTo(ranges.getOrderedRanges());
            if (ranges.getDiscreteSetMarker() == DISCRETE) {
                // discrete sets come back marked discrete; the block layout is whatever the block serde wrote,
                // exactly as on the JSON control plane
                assertThat(decodedRanges.getDiscreteSetMarker()).isEqualTo(DISCRETE);
            }
            assertThat(decodedRanges.isDiscreteSet()).isEqualTo(ranges.isDiscreteSet());
        }
        if (domain.getValues() instanceof EquatableValueSet values) {
            assertThat(((EquatableValueSet) decoded.getValues()).getEntries()).isEqualTo(values.getEntries());
        }
    }

    private static ByteString serialize(Type type, Object... values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            writeNativeValue(type, builder, value);
        }
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        SERDE.writeBlock(output, builder.build());
        return ByteString.copyFrom(output.slice().getBytes());
    }

    private static Object mapOf(long... keysAndValues)
    {
        BlockBuilder builder = BIGINT_MAP.createBlockBuilder(null, 1);
        ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
            for (int i = 0; i < keysAndValues.length; i += 2) {
                BIGINT.writeLong(keyBuilder, keysAndValues[i]);
                BIGINT.writeLong(valueBuilder, keysAndValues[i + 1]);
            }
        });
        return BIGINT_MAP.getObject(builder.build(), 0);
    }
}
