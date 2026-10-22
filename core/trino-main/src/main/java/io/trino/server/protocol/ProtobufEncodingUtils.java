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
package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.spooling.encoding.protobuf.ProtobufWireWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.VariantType;
import io.trino.spi.variant.Variant;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.IpAddressType;
import io.trino.type.JsonType;
import io.trino.type.LongInterval;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;
import io.trino.util.variant.VariantUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Verify.verify;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.ARRAY_ELEMENTS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_SCALE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_UNSCALED_BYTES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_UNSCALED_LONG;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_FRACTIONAL_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_MICROS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_PICOS_OF_MICRO;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.MAP_KEYS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.MAP_VALUES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NULL_VALUE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_DECIMAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_NEGATIVE_INFINITY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_NOT_A_NUMBER;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_POSITIVE_INFINITY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.ROW_VALUES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.SEGMENT_ROWS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_EPOCH_MICROS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_PICOS_OF_MICRO;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_WITH_TIME_ZONE_EPOCH_MILLIS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_WITH_TIME_ZONE_OFFSET_SECONDS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_WITH_TIME_ZONE_PICOS_OF_MILLI;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_WITH_TIME_ZONE_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIMESTAMP_WITH_TIME_ZONE_ZONE_ID;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIME_PICOS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIME_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIME_WITH_TIME_ZONE_OFFSET_MINUTES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIME_WITH_TIME_ZONE_PICOS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.TIME_WITH_TIME_ZONE_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_ARRAY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_BIGINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_BOOLEAN;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_CHAR;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_DATE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_DECIMAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_DOUBLE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_INTEGER;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_INTERVAL_DAY_TO_SECOND;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_IPADDRESS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_JSON;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_JSON_OBJECT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_MAP;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_NULL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_NUMBER;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_REAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_ROW;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_SMALLINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TEXT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TIME;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TIMESTAMP;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TIME_WITH_TIME_ZONE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_TINYINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_UUID;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_VARBINARY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_VARCHAR;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VALUE_VARIANT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VARIANT_BINARY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VARIANT_BINARY_METADATA;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VARIANT_BINARY_VALUE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.VARIANT_JSON;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Writes pages in the protobuf spooled segment format defined in {@code trino_spooled_segment.proto}.
 * Every value is written from the block directly; nothing goes through JSON except the handful of
 * types whose only client representation is a JSON document.
 */
public final class ProtobufEncodingUtils
{
    private ProtobufEncodingUtils() {}

    private static final int LEGACY_PRECISION = 3;

    private static final TypeEncoder BOOLEAN_ENCODER = new BooleanEncoder();
    private static final TypeEncoder TINYINT_ENCODER = new TinyintEncoder();
    private static final TypeEncoder SMALLINT_ENCODER = new SmallintEncoder();
    private static final TypeEncoder INTEGER_ENCODER = new IntegerEncoder();
    private static final TypeEncoder BIGINT_ENCODER = new BigintEncoder();
    private static final TypeEncoder REAL_ENCODER = new RealEncoder();
    private static final TypeEncoder DOUBLE_ENCODER = new DoubleEncoder();
    private static final TypeEncoder VARCHAR_ENCODER = new VarcharEncoder();
    private static final TypeEncoder VARBINARY_ENCODER = new VarbinaryEncoder();
    private static final TypeEncoder JSON_ENCODER = new JsonEncoder();
    private static final TypeEncoder DATE_ENCODER = new DateEncoder();
    private static final TypeEncoder UUID_ENCODER = new UuidEncoder();
    private static final TypeEncoder IPADDRESS_ENCODER = new IpAddressEncoder();
    private static final TypeEncoder INTERVAL_YEAR_MONTH_ENCODER = new IntervalYearMonthEncoder();

    public static TypeEncoder[] createTypeEncoders(Session session, List<Type> types)
    {
        verify(!types.isEmpty(), "Columns must not be empty");

        Set<String> clientCapabilities = session.getClientCapabilities();
        Capabilities capabilities = new Capabilities(
                clientCapabilities.contains(ClientCapabilities.PARAMETRIC_DATETIME.toString()),
                clientCapabilities.contains(ClientCapabilities.PARAMETRIC_INTERVAL.toString()),
                clientCapabilities.contains(ClientCapabilities.NUMBER.toString()),
                clientCapabilities.contains(ClientCapabilities.VARIANT.toString()),
                clientCapabilities.contains(ClientCapabilities.VARIANT_BINARY.toString()));

        return types.stream()
                .map(type -> createTypeEncoder(type, capabilities))
                .toArray(TypeEncoder[]::new);
    }

    /**
     * The client capabilities that change what a column looks like to the client, and so what the
     * encoder must send: the same adjustments the json encoding makes.
     */
    public record Capabilities(
            boolean supportsParametricDateTime,
            boolean supportsParametricInterval,
            boolean supportsNumber,
            boolean supportsVariant,
            boolean supportsVariantBinary)
    {
        public static Capabilities all()
        {
            return new Capabilities(true, true, true, true, true);
        }
    }

    public static TypeEncoder createTypeEncoder(Type type, Capabilities capabilities)
    {
        return switch (type) {
            case BooleanType _ -> BOOLEAN_ENCODER;
            case TinyintType _ -> TINYINT_ENCODER;
            case SmallintType _ -> SMALLINT_ENCODER;
            case IntegerType _ -> INTEGER_ENCODER;
            case BigintType _ -> BIGINT_ENCODER;
            case RealType _ -> REAL_ENCODER;
            case DoubleType _ -> DOUBLE_ENCODER;
            case VarcharType _ -> VARCHAR_ENCODER;
            case CharType _ -> new CharEncoder();
            case VarbinaryType _ -> VARBINARY_ENCODER;
            case JsonType _ -> JSON_ENCODER;
            case DateType _ -> DATE_ENCODER;
            case UuidType _ -> UUID_ENCODER;
            case IpAddressType _ -> IPADDRESS_ENCODER;
            case DecimalType decimalType -> decimalType.isShort() ? new ShortDecimalEncoder(decimalType) : new LongDecimalEncoder(decimalType);
            case TimeType timeType when capabilities.supportsParametricDateTime() -> new TimeEncoder(timeType);
            case TimeWithTimeZoneType timeType when capabilities.supportsParametricDateTime() -> new TimeWithTimeZoneEncoder(timeType);
            case TimestampType timestampType when capabilities.supportsParametricDateTime() -> new TimestampEncoder(timestampType);
            case TimestampWithTimeZoneType timestampType when capabilities.supportsParametricDateTime() -> new TimestampWithTimeZoneEncoder(timestampType);
            case IntervalYearMonthType _ -> INTERVAL_YEAR_MONTH_ENCODER;
            case IntervalDayTimeType intervalType when capabilities.supportsParametricInterval() -> new IntervalDayTimeEncoder(intervalType);
            case NumberType numberType -> new NumberEncoder(numberType, capabilities.supportsNumber());
            case VariantType _ -> new VariantEncoder(capabilities.supportsVariant(), capabilities.supportsVariantBinary());
            case ArrayType arrayType -> new ArrayEncoder(arrayType, createTypeEncoder(arrayType.getElementType(), capabilities));
            case MapType mapType -> new MapEncoder(mapType, createTypeEncoder(mapType.getKeyType(), capabilities), createTypeEncoder(mapType.getValueType(), capabilities));
            case RowType rowType -> new RowEncoder(rowType, rowType.getFieldTypes().stream()
                    .map(fieldType -> createTypeEncoder(fieldType, capabilities))
                    .toArray(TypeEncoder[]::new));
            case Type _ -> new TypeObjectValueEncoder(type, capabilities);
        };
    }

    /**
     * Writes the rows of the pages as the {@code rows} of one {@code Segment} message. Each row goes to
     * the output as soon as it is complete, so the writer only ever holds one row.
     * <p>
     * The encoders are shared by every driver writing the query's segments, so they must not keep
     * per call state; the writer is local to this call.
     */
    public static void writePages(OutputStream output, TypeEncoder[] typeEncoders, int[] sourcePageChannels, List<Page> pages)
            throws IOException
    {
        verify(typeEncoders.length == sourcePageChannels.length, "Source page channels and type encoders must have the same length");
        ProtobufWireWriter rowWriter = new ProtobufWireWriter();
        Block[] blocks = new Block[sourcePageChannels.length];
        for (Page page : pages) {
            for (int i = 0; i < sourcePageChannels.length; i++) {
                blocks[i] = page.getBlock(sourcePageChannels[i]);
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                rowWriter.reset();
                for (int column = 0; column < typeEncoders.length; column++) {
                    writeValue(rowWriter, ROW_VALUES, typeEncoders[column], blocks[column], position);
                }
                ProtobufWireWriter.writeMessageTo(output, SEGMENT_ROWS, rowWriter);
            }
        }
    }

    /**
     * Writes one {@code Value} message as the given field of the enclosing message.
     */
    private static void writeValue(ProtobufWireWriter writer, int fieldNumber, TypeEncoder encoder, Block block, int position)
    {
        ProtobufWireWriter valueWriter = writer.child();
        if (block.isNull(position)) {
            valueWriter.writeEnum(VALUE_NULL, NULL_VALUE);
        }
        else {
            encoder.encode(valueWriter, block, position);
        }
        writer.writeMessage(fieldNumber, valueWriter);
    }

    /**
     * Writes the single {@code kind} field of a {@code Value} message for a non-null position.
     */
    public sealed interface TypeEncoder
    {
        void encode(ProtobufWireWriter writer, Block block, int position);
    }

    private static final class BooleanEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeBool(VALUE_BOOLEAN, BOOLEAN.getBoolean(block, position));
        }
    }

    private static final class TinyintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint32(VALUE_TINYINT, TINYINT.getByte(block, position));
        }
    }

    private static final class SmallintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint32(VALUE_SMALLINT, SMALLINT.getShort(block, position));
        }
    }

    private static final class IntegerEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint32(VALUE_INTEGER, INTEGER.getInt(block, position));
        }
    }

    private static final class BigintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint64(VALUE_BIGINT, BIGINT.getLong(block, position));
        }
    }

    private static final class RealEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeFloat(VALUE_REAL, REAL.getFloat(block, position));
        }
    }

    private static final class DoubleEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeDouble(VALUE_DOUBLE, DOUBLE.getDouble(block, position));
        }
    }

    private static final class VarcharEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeSlice(writer, VALUE_VARCHAR, VARCHAR.getSlice(block, position));
        }
    }

    private static final class CharEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            // Stored without trailing spaces; the client pads to the declared length
            writeSlice(writer, VALUE_CHAR, VARCHAR.getSlice(block, position));
        }
    }

    private static final class VarbinaryEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeSlice(writer, VALUE_VARBINARY, VARBINARY.getSlice(block, position));
        }
    }

    private static final class JsonEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeSlice(writer, VALUE_JSON, JSON.getSlice(block, position));
        }
    }

    private static final class DateEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint32(VALUE_DATE, DATE.getInt(block, position));
        }
    }

    private static final class UuidEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            // Stored in network byte order already
            writeSlice(writer, VALUE_UUID, UUID.getSlice(block, position));
        }
    }

    private static final class IpAddressEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeSlice(writer, VALUE_IPADDRESS, IPADDRESS.getSlice(block, position));
        }
    }

    private static final class ShortDecimalEncoder
            implements TypeEncoder
    {
        private final DecimalType type;

        private ShortDecimalEncoder(DecimalType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            ProtobufWireWriter decimal = writer.child();
            decimal.writeSint64(DECIMAL_UNSCALED_LONG, type.getLong(block, position));
            decimal.writeSint32(DECIMAL_SCALE, type.getScale());
            writer.writeMessage(VALUE_DECIMAL, decimal);
        }
    }

    private static final class LongDecimalEncoder
            implements TypeEncoder
    {
        private final DecimalType type;

        private LongDecimalEncoder(DecimalType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeDecimal(writer, VALUE_DECIMAL, (Int128) type.getObject(block, position), type.getScale());
        }
    }

    private static void writeDecimal(ProtobufWireWriter writer, int fieldNumber, Int128 unscaled, int scale)
    {
        ProtobufWireWriter decimal = writer.child();
        long low = unscaled.getLow();
        if (unscaled.getHigh() == (low >> 63)) {
            decimal.writeSint64(DECIMAL_UNSCALED_LONG, low);
        }
        else {
            decimal.writeBytes(DECIMAL_UNSCALED_BYTES, unscaled.toBigInteger().toByteArray());
        }
        decimal.writeSint32(DECIMAL_SCALE, scale);
        writer.writeMessage(fieldNumber, decimal);
    }

    private static void writeDecimal(ProtobufWireWriter writer, int fieldNumber, BigInteger unscaled, int scale)
    {
        ProtobufWireWriter decimal = writer.child();
        if (unscaled.bitLength() < 64) {
            decimal.writeSint64(DECIMAL_UNSCALED_LONG, unscaled.longValueExact());
        }
        else {
            decimal.writeBytes(DECIMAL_UNSCALED_BYTES, unscaled.toByteArray());
        }
        decimal.writeSint32(DECIMAL_SCALE, scale);
        writer.writeMessage(fieldNumber, decimal);
    }

    private static final class TimeEncoder
            implements TypeEncoder
    {
        private final TimeType type;

        private TimeEncoder(TimeType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writeTime(writer, type.getLong(block, position), type.getPrecision());
        }
    }

    private static void writeTime(ProtobufWireWriter writer, long picos, int precision)
    {
        ProtobufWireWriter time = writer.child();
        time.writeInt64(TIME_PICOS, picos);
        time.writeInt32(TIME_PRECISION, precision);
        writer.writeMessage(VALUE_TIME, time);
    }

    private static final class TimeWithTimeZoneEncoder
            implements TypeEncoder
    {
        private final TimeWithTimeZoneType type;

        private TimeWithTimeZoneEncoder(TimeWithTimeZoneType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            if (type.isShort()) {
                long packed = type.getLong(block, position);
                writeTimeWithTimeZone(writer, unpackTimeNanos(packed) * PICOSECONDS_PER_NANOSECOND, type.getPrecision(), unpackOffsetMinutes(packed));
            }
            else {
                LongTimeWithTimeZone value = (LongTimeWithTimeZone) type.getObject(block, position);
                writeTimeWithTimeZone(writer, value.getPicoseconds(), type.getPrecision(), value.getOffsetMinutes());
            }
        }
    }

    private static void writeTimeWithTimeZone(ProtobufWireWriter writer, long picos, int precision, int offsetMinutes)
    {
        ProtobufWireWriter time = writer.child();
        time.writeInt64(TIME_WITH_TIME_ZONE_PICOS, picos);
        time.writeInt32(TIME_WITH_TIME_ZONE_PRECISION, precision);
        time.writeSint32(TIME_WITH_TIME_ZONE_OFFSET_MINUTES, offsetMinutes);
        writer.writeMessage(VALUE_TIME_WITH_TIME_ZONE, time);
    }

    private static final class TimestampEncoder
            implements TypeEncoder
    {
        private final TimestampType type;

        private TimestampEncoder(TimestampType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            if (type.isShort()) {
                writeTimestamp(writer, type.getLong(block, position), 0, type.getPrecision());
            }
            else {
                LongTimestamp value = (LongTimestamp) type.getObject(block, position);
                writeTimestamp(writer, value.getEpochMicros(), value.getPicosOfMicro(), type.getPrecision());
            }
        }
    }

    private static void writeTimestamp(ProtobufWireWriter writer, long epochMicros, int picosOfMicro, int precision)
    {
        ProtobufWireWriter timestamp = writer.child();
        timestamp.writeSint64(TIMESTAMP_EPOCH_MICROS, epochMicros);
        if (picosOfMicro != 0) {
            timestamp.writeInt32(TIMESTAMP_PICOS_OF_MICRO, picosOfMicro);
        }
        timestamp.writeInt32(TIMESTAMP_PRECISION, precision);
        writer.writeMessage(VALUE_TIMESTAMP, timestamp);
    }

    private static final class TimestampWithTimeZoneEncoder
            implements TypeEncoder
    {
        private final TimestampWithTimeZoneType type;
        private final Zones zones = new Zones();

        private TimestampWithTimeZoneEncoder(TimestampWithTimeZoneType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            if (type.isShort()) {
                long packed = type.getLong(block, position);
                writeTimestampWithTimeZone(writer, unpackMillisUtc(packed), 0, type.getPrecision(), zones.get(unpackZoneKey(packed)));
            }
            else {
                LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) type.getObject(block, position);
                writeTimestampWithTimeZone(writer, value.getEpochMillis(), value.getPicosOfMilli(), type.getPrecision(), zones.get(TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey())));
            }
        }
    }

    /**
     * The zones of the keys seen by one encoder; a column usually has only a few. Encoders are shared
     * by the drivers writing a query's segments, so the cache is concurrent.
     */
    private static final class Zones
    {
        private final Map<TimeZoneKey, Zone> zones = new ConcurrentHashMap<>();

        public Zone get(TimeZoneKey key)
        {
            return zones.computeIfAbsent(key, _ -> new Zone(key.getZoneId().getId().getBytes(UTF_8), key.getZoneId().getRules()));
        }
    }

    private record Zone(byte[] id, ZoneRules rules) {}

    private static void writeTimestampWithTimeZone(ProtobufWireWriter writer, long epochMillis, int picosOfMilli, int precision, Zone zone)
    {
        ProtobufWireWriter timestamp = writer.child();
        timestamp.writeSint64(TIMESTAMP_WITH_TIME_ZONE_EPOCH_MILLIS, epochMillis);
        if (picosOfMilli != 0) {
            timestamp.writeInt32(TIMESTAMP_WITH_TIME_ZONE_PICOS_OF_MILLI, picosOfMilli);
        }
        timestamp.writeInt32(TIMESTAMP_WITH_TIME_ZONE_PRECISION, precision);
        timestamp.writeBytes(TIMESTAMP_WITH_TIME_ZONE_ZONE_ID, zone.id());
        // The offset is resolved here, with the rules the json encoding formats the value with, so a client
        // renders the same local date-time without a time zone database of its own
        int offsetSeconds = zone.rules().getOffset(Instant.ofEpochMilli(epochMillis)).getTotalSeconds();
        if (offsetSeconds != 0) {
            timestamp.writeSint32(TIMESTAMP_WITH_TIME_ZONE_OFFSET_SECONDS, offsetSeconds);
        }
        writer.writeMessage(VALUE_TIMESTAMP_WITH_TIME_ZONE, timestamp);
    }

    private static final class IntervalYearMonthEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            writer.writeSint32(VALUE_INTERVAL_YEAR_TO_MONTH, INTERVAL_YEAR_MONTH.getInt(block, position));
        }
    }

    private static final class IntervalDayTimeEncoder
            implements TypeEncoder
    {
        private final IntervalDayTimeType type;

        private IntervalDayTimeEncoder(IntervalDayTimeType type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            if (type.isShort()) {
                writeIntervalDayTime(writer, type.getLong(block, position), 0, type.getFractionalPrecision());
            }
            else {
                LongInterval value = (LongInterval) type.getObject(block, position);
                writeIntervalDayTime(writer, value.getMicros(), value.getPicosOfMicro(), type.getFractionalPrecision());
            }
        }
    }

    private static void writeIntervalDayTime(ProtobufWireWriter writer, long micros, int picosOfMicro, int fractionalPrecision)
    {
        ProtobufWireWriter interval = writer.child();
        interval.writeSint64(INTERVAL_DAY_TO_SECOND_MICROS, micros);
        if (picosOfMicro != 0) {
            interval.writeInt32(INTERVAL_DAY_TO_SECOND_PICOS_OF_MICRO, picosOfMicro);
        }
        interval.writeInt32(INTERVAL_DAY_TO_SECOND_FRACTIONAL_PRECISION, fractionalPrecision);
        writer.writeMessage(VALUE_INTERVAL_DAY_TO_SECOND, interval);
    }

    private static final class NumberEncoder
            implements TypeEncoder
    {
        private final NumberType type;
        private final boolean supportsNumber;

        private NumberEncoder(NumberType type, boolean supportsNumber)
        {
            this.type = requireNonNull(type, "type is null");
            this.supportsNumber = supportsNumber;
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            TrinoNumber.AsBigDecimal value = ((TrinoNumber) type.getObject(block, position)).toBigDecimal();
            writeNumber(writer, value, supportsNumber);
        }
    }

    private static void writeNumber(ProtobufWireWriter writer, TrinoNumber.AsBigDecimal value, boolean supportsNumber)
    {
        if (!supportsNumber) {
            // The column is a varchar to this client
            writer.writeString(VALUE_VARCHAR, value.toString());
            return;
        }
        ProtobufWireWriter number = writer.child();
        switch (value) {
            case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> writeDecimal(number, NUMBER_DECIMAL, decimal.unscaledValue(), decimal.scale());
            case TrinoNumber.NotANumber _ -> number.writeEnum(NUMBER_SPECIAL, NUMBER_SPECIAL_NOT_A_NUMBER);
            case TrinoNumber.Infinity(boolean negative) -> number.writeEnum(NUMBER_SPECIAL, negative ? NUMBER_SPECIAL_NEGATIVE_INFINITY : NUMBER_SPECIAL_POSITIVE_INFINITY);
        }
        writer.writeMessage(VALUE_NUMBER, number);
    }

    private static final class VariantEncoder
            implements TypeEncoder
    {
        private final boolean supportsVariant;
        private final boolean supportsVariantBinary;

        private VariantEncoder(boolean supportsVariant, boolean supportsVariantBinary)
        {
            this.supportsVariant = supportsVariant;
            this.supportsVariantBinary = supportsVariantBinary;
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            Variant value = VARIANT.getObject(block, position);
            if (supportsVariantBinary) {
                ProtobufWireWriter variant = writer.child();
                ProtobufWireWriter binary = variant.child();
                writeSlice(binary, VARIANT_BINARY_METADATA, value.metadata().toSlice());
                writeSlice(binary, VARIANT_BINARY_VALUE, value.data());
                variant.writeMessage(VARIANT_BINARY, binary);
                writer.writeMessage(VALUE_VARIANT, variant);
            }
            else if (supportsVariant) {
                ProtobufWireWriter variant = writer.child();
                writeSlice(variant, VARIANT_JSON, VariantUtil.asJson(value));
                writer.writeMessage(VALUE_VARIANT, variant);
            }
            else {
                // The column is a json to this client
                writeSlice(writer, VALUE_JSON, VariantUtil.asJson(value));
            }
        }
    }

    private static final class ArrayEncoder
            implements TypeEncoder
    {
        private final ArrayType type;
        private final TypeEncoder elementEncoder;

        private ArrayEncoder(ArrayType type, TypeEncoder elementEncoder)
        {
            this.type = requireNonNull(type, "type is null");
            this.elementEncoder = requireNonNull(elementEncoder, "elementEncoder is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            Block elements = type.getObject(block, position);
            ProtobufWireWriter array = writer.child();
            for (int i = 0; i < elements.getPositionCount(); i++) {
                writeValue(array, ARRAY_ELEMENTS, elementEncoder, elements, i);
            }
            writer.writeMessage(VALUE_ARRAY, array);
        }
    }

    private static final class MapEncoder
            implements TypeEncoder
    {
        private final MapType type;
        private final TypeEncoder keyEncoder;
        private final TypeEncoder valueEncoder;

        private MapEncoder(MapType type, TypeEncoder keyEncoder, TypeEncoder valueEncoder)
        {
            this.type = requireNonNull(type, "type is null");
            this.keyEncoder = requireNonNull(keyEncoder, "keyEncoder is null");
            this.valueEncoder = requireNonNull(valueEncoder, "valueEncoder is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            SqlMap sqlMap = type.getObject(block, position);
            int offset = sqlMap.getRawOffset();
            Block keys = sqlMap.getRawKeyBlock();
            Block values = sqlMap.getRawValueBlock();
            ProtobufWireWriter map = writer.child();
            for (int i = 0; i < sqlMap.getSize(); i++) {
                writeValue(map, MAP_KEYS, keyEncoder, keys, offset + i);
            }
            for (int i = 0; i < sqlMap.getSize(); i++) {
                writeValue(map, MAP_VALUES, valueEncoder, values, offset + i);
            }
            writer.writeMessage(VALUE_MAP, map);
        }
    }

    private static final class RowEncoder
            implements TypeEncoder
    {
        private final RowType type;
        private final TypeEncoder[] fieldEncoders;

        private RowEncoder(RowType type, TypeEncoder[] fieldEncoders)
        {
            this.type = requireNonNull(type, "type is null");
            this.fieldEncoders = requireNonNull(fieldEncoders, "fieldEncoders is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            SqlRow sqlRow = type.getObject(block, position);
            ProtobufWireWriter row = writer.child();
            for (int i = 0; i < fieldEncoders.length; i++) {
                writeValue(row, ROW_VALUES, fieldEncoders[i], sqlRow.getRawFieldBlock(i), sqlRow.getRawIndex());
            }
            writer.writeMessage(VALUE_ROW, row);
        }
    }

    /**
     * Encodes through {@link Type#getObjectValue}: the temporal types when the client wants them rounded
     * to the legacy millisecond precision, and the types without a dedicated encoder. The latter are sent
     * as text or bytes when that is what the client sees, otherwise as the JSON document the json encoding
     * would have produced.
     */
    private static final class TypeObjectValueEncoder
            implements TypeEncoder
    {
        private static final JsonMapper JSON_MAPPER = new JsonMapper();

        private final Type type;
        private final Capabilities capabilities;
        private final Zones zones = new Zones();

        private TypeObjectValueEncoder(Type type, Capabilities capabilities)
        {
            this.type = requireNonNull(type, "type is null");
            this.capabilities = requireNonNull(capabilities, "capabilities is null");
        }

        @Override
        public void encode(ProtobufWireWriter writer, Block block, int position)
        {
            Object value = type.getObjectValue(block, position);
            switch (value) {
                case SqlTimestamp timestamp -> {
                    SqlTimestamp rounded = capabilities.supportsParametricDateTime() ? timestamp : timestamp.roundTo(LEGACY_PRECISION);
                    writeTimestamp(writer, rounded.getEpochMicros(), rounded.getPicosOfMicros(), rounded.getPrecision());
                }
                case SqlTimestampWithTimeZone timestamp -> {
                    SqlTimestampWithTimeZone rounded = capabilities.supportsParametricDateTime() ? timestamp : timestamp.roundTo(LEGACY_PRECISION);
                    writeTimestampWithTimeZone(writer, rounded.getEpochMillis(), rounded.getPicosOfMilli(), rounded.getPrecision(), zones.get(rounded.getTimeZoneKey()));
                }
                // Time and interval types have dedicated encoders when the client supports parametric values,
                // so these values only reach this encoder in the legacy, millisecond precision form
                case SqlTime time -> writeTime(writer, time.roundTo(LEGACY_PRECISION).getPicos(), LEGACY_PRECISION);
                case SqlTimeWithTimeZone time -> {
                    SqlTimeWithTimeZone rounded = time.roundTo(LEGACY_PRECISION);
                    writeTimeWithTimeZone(writer, rounded.getPicos(), LEGACY_PRECISION, rounded.getOffsetMinutes());
                }
                case SqlIntervalDayTime interval -> writeIntervalDayTime(writer, interval.roundTo(LEGACY_PRECISION).getMicros(), 0, LEGACY_PRECISION);
                case SqlIntervalYearMonth interval -> writer.writeSint32(VALUE_INTERVAL_YEAR_TO_MONTH, interval.getMonths());
                case SqlDate date -> writer.writeSint32(VALUE_DATE, date.getDays());
                case SqlDecimal decimal -> writeDecimal(writer, VALUE_DECIMAL, decimal.getUnscaledValue(), decimal.getScale());
                case SqlNumber number -> writeNumber(writer, number.value(), capabilities.supportsNumber());
                case SqlVarbinary varbinary -> writer.writeBytes(VALUE_VARBINARY, varbinary.getBytes());
                case String text -> writer.writeString(VALUE_TEXT, text);
                default -> {
                    try {
                        writer.writeString(VALUE_JSON_OBJECT, JSON_MAPPER.writeValueAsString(value));
                    }
                    catch (JsonProcessingException e) {
                        throw new TrinoException(SERIALIZATION_ERROR, "Could not serialize value of type " + type.getDisplayName(), e);
                    }
                }
            }
        }
    }

    private static void writeSlice(ProtobufWireWriter writer, int fieldNumber, Slice slice)
    {
        writer.writeBytes(fieldNumber, slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }
}
