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
package io.trino.client.spooling.encoding.protobuf;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.EncodedVariant;
import io.trino.client.IntervalDayTime;
import io.trino.client.IntervalYearMonth;
import io.trino.client.Row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.ClientStandardTypes.IPADDRESS;
import static io.trino.client.ClientStandardTypes.JSON;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.NUMBER;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TIME;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARBINARY;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.ClientStandardTypes.VARIANT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.ARRAY_ELEMENTS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_SCALE;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_UNSCALED_BYTES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.DECIMAL_UNSCALED_LONG;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_FRACTIONAL_PRECISION;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_LEGACY_MILLIS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_MICROS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.INTERVAL_DAY_TO_SECOND_PICOS_OF_MICRO;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.MAP_KEYS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.MAP_VALUES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_DECIMAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_NEGATIVE_INFINITY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_NOT_A_NUMBER;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.NUMBER_SPECIAL_POSITIVE_INFINITY;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.ROW_VALUES;
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
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED32;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED64;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_VARINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.fieldNumber;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.wireType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Decodes {@code Value} messages of the protobuf spooled segment format into the same Java
 * values that the json encoding produces, so the JDBC driver and CLI need no changes to use it.
 */
public final class ProtobufValueDecoders
{
    private static final long PICOSECONDS_PER_MICROSECOND = 1_000_000;
    private static final long PICOSECONDS_PER_MILLISECOND = 1_000_000_000;
    private static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    private static final long PICOSECONDS_PER_DAY = 24 * 60 * 60 * PICOSECONDS_PER_SECOND;
    private static final int MAX_PRECISION = 12;
    // java.time and Trino limits of zone offsets
    private static final int MAX_OFFSET_SECONDS = 18 * 60 * 60;
    private static final int MAX_TIME_ZONE_OFFSET_MINUTES = 14 * 60;

    private static final ValueDecoder BOOLEAN_DECODER = new BooleanDecoder();
    private static final ValueDecoder TINYINT_DECODER = new TinyintDecoder();
    private static final ValueDecoder SMALLINT_DECODER = new SmallintDecoder();
    private static final ValueDecoder INTEGER_DECODER = new IntegerDecoder();
    private static final ValueDecoder BIGINT_DECODER = new BigintDecoder();
    private static final ValueDecoder REAL_DECODER = new RealDecoder();
    private static final ValueDecoder DOUBLE_DECODER = new DoubleDecoder();
    private static final ValueDecoder DECIMAL_DECODER = new DecimalDecoder();
    private static final ValueDecoder VARCHAR_DECODER = new StringDecoder(VALUE_VARCHAR);
    private static final ValueDecoder JSON_DECODER = new StringDecoder(VALUE_JSON);
    private static final ValueDecoder VARBINARY_DECODER = new BytesDecoder(VALUE_VARBINARY);
    private static final ValueDecoder DATE_DECODER = new DateDecoder();
    private static final ValueDecoder TIME_DECODER = new TimeDecoder();
    private static final ValueDecoder TIME_WITH_TIME_ZONE_DECODER = new TimeWithTimeZoneDecoder();
    private static final ValueDecoder TIMESTAMP_DECODER = new TimestampDecoder();
    private static final ValueDecoder INTERVAL_YEAR_TO_MONTH_DECODER = new IntervalYearToMonthDecoder();
    private static final ValueDecoder INTERVAL_DAY_TO_SECOND_DECODER = new IntervalDayToSecondDecoder();
    private static final ValueDecoder UUID_DECODER = new UuidDecoder();
    private static final ValueDecoder IPADDRESS_DECODER = new IpAddressDecoder();
    private static final ValueDecoder NUMBER_DECODER = new NumberDecoder();
    private static final ValueDecoder VARIANT_BINARY_DECODER = new VariantBinaryDecoder();
    private static final ValueDecoder VARIANT_JSON_DECODER = new VariantJsonDecoder();

    private ProtobufValueDecoders() {}

    /**
     * Decodes the content of one {@code Value} message. The reader is positioned at the first byte
     * of the message and limited to its end; the decoder consumes it fully.
     */
    public interface ValueDecoder
    {
        Object decode(ProtobufWireReader reader);
    }

    public static ValueDecoder[] createValueDecoders(List<Column> columns, boolean supportsVariantBinary)
    {
        verify(!columns.isEmpty(), "Columns must not be empty");
        ValueDecoder[] decoders = new ValueDecoder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            decoders[i] = createValueDecoder(columns.get(i).getTypeSignature(), supportsVariantBinary);
        }
        return decoders;
    }

    public static ValueDecoder createValueDecoder(ClientTypeSignature signature, boolean supportsVariantBinary)
    {
        switch (signature.getRawType()) {
            case BOOLEAN:
                return BOOLEAN_DECODER;
            case TINYINT:
                return TINYINT_DECODER;
            case SMALLINT:
                return SMALLINT_DECODER;
            case INTEGER:
                return INTEGER_DECODER;
            case BIGINT:
                return BIGINT_DECODER;
            case REAL:
                return REAL_DECODER;
            case DOUBLE:
                return DOUBLE_DECODER;
            case DECIMAL:
                return DECIMAL_DECODER;
            case VARCHAR:
                return VARCHAR_DECODER;
            case CHAR:
                return new CharDecoder(signature);
            case JSON:
                return JSON_DECODER;
            case VARBINARY:
                return VARBINARY_DECODER;
            case DATE:
                return DATE_DECODER;
            case TIME:
                return TIME_DECODER;
            case TIME_WITH_TIME_ZONE:
                return TIME_WITH_TIME_ZONE_DECODER;
            case TIMESTAMP:
                return TIMESTAMP_DECODER;
            case TIMESTAMP_WITH_TIME_ZONE:
                // one per column: it caches the zone id of the last value
                return new TimestampWithTimeZoneDecoder();
            case INTERVAL_YEAR_TO_MONTH:
                return INTERVAL_YEAR_TO_MONTH_DECODER;
            case INTERVAL_DAY_TO_SECOND:
                return INTERVAL_DAY_TO_SECOND_DECODER;
            case UUID:
                return UUID_DECODER;
            case IPADDRESS:
                return IPADDRESS_DECODER;
            case NUMBER:
                return NUMBER_DECODER;
            case VARIANT:
                return supportsVariantBinary ? VARIANT_BINARY_DECODER : VARIANT_JSON_DECODER;
            case ARRAY:
                return new ArrayDecoder(signature, supportsVariantBinary);
            case MAP:
                return new MapDecoder(signature, supportsVariantBinary);
            case ROW:
                return new RowDecoder(signature, supportsVariantBinary);
            default:
                return new OtherTypeDecoder();
        }
    }

    /**
     * Reads a length-delimited {@code Value} and decodes it.
     */
    static Object readValue(ProtobufWireReader reader, ValueDecoder decoder)
    {
        int previousLimit = reader.pushLimit(reader.readLength());
        Object value = decoder.decode(reader);
        reader.popLimit(previousLimit);
        return value;
    }

    /**
     * Reads the tag of the single {@code kind} field of a {@code Value}. Returns -1 for a NULL (consumed).
     */
    private static int readKind(ProtobufWireReader reader)
    {
        if (!reader.hasRemaining()) {
            throw new ProtobufFormatException("Value has no kind set");
        }
        int tag = reader.readTag();
        if (fieldNumber(tag) == VALUE_NULL) {
            expectWireType(tag, WIRETYPE_VARINT);
            reader.readVarint32();
            return -1;
        }
        return tag;
    }

    private static void expectKind(int tag, int expectedField, int expectedWireType)
    {
        if (fieldNumber(tag) != expectedField) {
            throw new ProtobufFormatException(format("Expected value field %s but got %s", expectedField, fieldNumber(tag)));
        }
        expectWireType(tag, expectedWireType);
    }

    private static void expectWireType(int tag, int expectedWireType)
    {
        if (wireType(tag) != expectedWireType) {
            throw new ProtobufFormatException(format("Expected wire type %s for field %s but got %s", expectedWireType, fieldNumber(tag), wireType(tag)));
        }
    }

    private abstract static class ScalarDecoder
            implements ValueDecoder
    {
        private final int field;
        private final int wireType;

        private ScalarDecoder(int field, int wireType)
        {
            this.field = field;
            this.wireType = wireType;
        }

        @Override
        public final Object decode(ProtobufWireReader reader)
        {
            int tag = readKind(reader);
            if (tag == -1) {
                return null;
            }
            expectKind(tag, field, wireType);
            return decodeValue(reader);
        }

        abstract Object decodeValue(ProtobufWireReader reader);
    }

    private static class BooleanDecoder
            extends ScalarDecoder
    {
        BooleanDecoder()
        {
            super(VALUE_BOOLEAN, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readBool();
        }
    }

    private static class TinyintDecoder
            extends ScalarDecoder
    {
        TinyintDecoder()
        {
            super(VALUE_TINYINT, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            int value = reader.readSint32();
            if (value != (byte) value) {
                throw new ProtobufFormatException("Tinyint out of range: " + value);
            }
            return (byte) value;
        }
    }

    private static class SmallintDecoder
            extends ScalarDecoder
    {
        SmallintDecoder()
        {
            super(VALUE_SMALLINT, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            int value = reader.readSint32();
            if (value != (short) value) {
                throw new ProtobufFormatException("Smallint out of range: " + value);
            }
            return (short) value;
        }
    }

    private static class IntegerDecoder
            extends ScalarDecoder
    {
        IntegerDecoder()
        {
            super(VALUE_INTEGER, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readSint32();
        }
    }

    private static class BigintDecoder
            extends ScalarDecoder
    {
        BigintDecoder()
        {
            super(VALUE_BIGINT, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readSint64();
        }
    }

    private static class RealDecoder
            extends ScalarDecoder
    {
        RealDecoder()
        {
            super(VALUE_REAL, WIRETYPE_FIXED32);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readFloat();
        }
    }

    private static class DoubleDecoder
            extends ScalarDecoder
    {
        DoubleDecoder()
        {
            super(VALUE_DOUBLE, WIRETYPE_FIXED64);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readDouble();
        }
    }

    private static class StringDecoder
            extends ScalarDecoder
    {
        StringDecoder(int field)
        {
            super(field, WIRETYPE_LENGTH_DELIMITED);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readString(reader.readLength());
        }
    }

    private static class BytesDecoder
            extends ScalarDecoder
    {
        BytesDecoder(int field)
        {
            super(field, WIRETYPE_LENGTH_DELIMITED);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return reader.readBytes(reader.readLength());
        }
    }

    private static class CharDecoder
            extends ScalarDecoder
    {
        private final int length;

        CharDecoder(ClientTypeSignature signature)
        {
            super(VALUE_CHAR, WIRETYPE_LENGTH_DELIMITED);
            // char without a declared length is char(1)
            this.length = signature.getArguments().isEmpty() ? 1 : toIntExact(signature.getArguments().get(0).getLongLiteral());
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            String value = reader.readString(reader.readLength());
            int codePoints = value.codePointCount(0, value.length());
            if (codePoints >= length) {
                return value;
            }
            StringBuilder builder = new StringBuilder(value.length() + length - codePoints);
            builder.append(value);
            for (int i = codePoints; i < length; i++) {
                builder.append(' ');
            }
            return builder.toString();
        }
    }

    private static class DateDecoder
            extends ScalarDecoder
    {
        DateDecoder()
        {
            super(VALUE_DATE, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return ProtobufValueFormatter.formatDate(reader.readSint32());
        }
    }

    private static class IntervalYearToMonthDecoder
            extends ScalarDecoder
    {
        IntervalYearToMonthDecoder()
        {
            super(VALUE_INTERVAL_YEAR_TO_MONTH, WIRETYPE_VARINT);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            return IntervalYearMonth.formatMonths(reader.readSint32());
        }
    }

    private static class UuidDecoder
            extends ScalarDecoder
    {
        UuidDecoder()
        {
            super(VALUE_UUID, WIRETYPE_LENGTH_DELIMITED);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            int length = reader.readLength();
            if (length != 16) {
                throw new ProtobufFormatException("Expected 16 bytes for uuid but got " + length);
            }
            long high = readBigEndianLong(reader);
            long low = readBigEndianLong(reader);
            return new UUID(high, low).toString();
        }

        private static long readBigEndianLong(ProtobufWireReader reader)
        {
            return Long.reverseBytes(reader.readFixed64());
        }
    }

    private static class IpAddressDecoder
            extends ScalarDecoder
    {
        IpAddressDecoder()
        {
            super(VALUE_IPADDRESS, WIRETYPE_LENGTH_DELIMITED);
        }

        @Override
        Object decodeValue(ProtobufWireReader reader)
        {
            int length = reader.readLength();
            if (length != 16) {
                throw new ProtobufFormatException("Expected 16 bytes for ipaddress but got " + length);
            }
            try {
                return InetAddresses.toAddrString(InetAddress.getByAddress(reader.readBytes(length)));
            }
            catch (UnknownHostException e) {
                throw new ProtobufFormatException("Invalid ipaddress value");
            }
        }
    }

    /**
     * Decodes a nested message field: {@code kind} is length-delimited and the message content is read
     * field by field, in any order, unknown fields skipped.
     */
    private abstract static class MessageDecoder
            extends ScalarDecoder
    {
        private MessageDecoder(int field)
        {
            super(field, WIRETYPE_LENGTH_DELIMITED);
        }

        @Override
        final Object decodeValue(ProtobufWireReader reader)
        {
            int previousLimit = reader.pushLimit(reader.readLength());
            Object value = decodeMessage(reader);
            reader.popLimit(previousLimit);
            return value;
        }

        abstract Object decodeMessage(ProtobufWireReader reader);
    }

    private static class DecimalDecoder
            extends MessageDecoder
    {
        DecimalDecoder()
        {
            super(VALUE_DECIMAL);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            boolean hasUnscaled = false;
            long unscaledLong = 0;
            byte[] unscaledBytes = null;
            int scale = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case DECIMAL_UNSCALED_LONG:
                        expectWireType(tag, WIRETYPE_VARINT);
                        unscaledLong = reader.readSint64();
                        unscaledBytes = null;
                        hasUnscaled = true;
                        break;
                    case DECIMAL_UNSCALED_BYTES:
                        expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                        unscaledBytes = readUnscaledBytes(reader);
                        hasUnscaled = true;
                        break;
                    case DECIMAL_SCALE:
                        expectWireType(tag, WIRETYPE_VARINT);
                        scale = reader.readSint32();
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            if (!hasUnscaled) {
                throw new ProtobufFormatException("Decimal has no unscaled value");
            }
            if (scale < 0) {
                throw new ProtobufFormatException("Invalid decimal scale: " + scale);
            }
            if (unscaledBytes == null) {
                return ProtobufValueFormatter.formatDecimal(unscaledLong, scale);
            }
            return ProtobufValueFormatter.formatDecimal(unscaledBytes, scale);
        }
    }

    private static byte[] readUnscaledBytes(ProtobufWireReader reader)
    {
        byte[] bytes = reader.readBytes(reader.readLength());
        if (bytes.length == 0) {
            throw new ProtobufFormatException("Decimal has empty unscaled bytes");
        }
        return bytes;
    }

    /**
     * Reads a {@code Decimal} message as a {@code BigDecimal}, for the types that render through one.
     */
    private static BigDecimal readBigDecimal(ProtobufWireReader reader)
    {
        BigInteger unscaled = null;
        int scale = 0;
        while (reader.hasRemaining()) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case DECIMAL_UNSCALED_LONG:
                    expectWireType(tag, WIRETYPE_VARINT);
                    unscaled = BigInteger.valueOf(reader.readSint64());
                    break;
                case DECIMAL_UNSCALED_BYTES:
                    expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                    unscaled = new BigInteger(readUnscaledBytes(reader));
                    break;
                case DECIMAL_SCALE:
                    expectWireType(tag, WIRETYPE_VARINT);
                    scale = reader.readSint32();
                    break;
                default:
                    reader.skipField(tag);
            }
        }
        if (unscaled == null) {
            throw new ProtobufFormatException("Decimal has no unscaled value");
        }
        return new BigDecimal(unscaled, scale);
    }

    private static class TimeDecoder
            extends MessageDecoder
    {
        TimeDecoder()
        {
            super(VALUE_TIME);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            long picos = 0;
            int precision = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case TIME_PICOS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        picos = reader.readVarint64();
                        break;
                    case TIME_PRECISION:
                        expectWireType(tag, WIRETYPE_VARINT);
                        precision = reader.readVarint32();
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            checkRange("picos", picos, 0, PICOSECONDS_PER_DAY - 1);
            return ProtobufValueFormatter.formatTime(checkPrecision(precision), picos);
        }
    }

    private static class TimeWithTimeZoneDecoder
            extends MessageDecoder
    {
        TimeWithTimeZoneDecoder()
        {
            super(VALUE_TIME_WITH_TIME_ZONE);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            long picos = 0;
            int precision = 0;
            int offsetMinutes = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case TIME_WITH_TIME_ZONE_PICOS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        picos = reader.readVarint64();
                        break;
                    case TIME_WITH_TIME_ZONE_PRECISION:
                        expectWireType(tag, WIRETYPE_VARINT);
                        precision = reader.readVarint32();
                        break;
                    case TIME_WITH_TIME_ZONE_OFFSET_MINUTES:
                        expectWireType(tag, WIRETYPE_VARINT);
                        offsetMinutes = reader.readSint32();
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            checkRange("picos", picos, 0, PICOSECONDS_PER_DAY - 1);
            checkRange("offset minutes", offsetMinutes, -MAX_TIME_ZONE_OFFSET_MINUTES, MAX_TIME_ZONE_OFFSET_MINUTES);
            return ProtobufValueFormatter.formatTimeWithTimeZone(checkPrecision(precision), picos, offsetMinutes);
        }
    }

    private static class TimestampDecoder
            extends MessageDecoder
    {
        TimestampDecoder()
        {
            super(VALUE_TIMESTAMP);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            long epochMicros = 0;
            int picosOfMicro = 0;
            int precision = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case TIMESTAMP_EPOCH_MICROS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        epochMicros = reader.readSint64();
                        break;
                    case TIMESTAMP_PICOS_OF_MICRO:
                        expectWireType(tag, WIRETYPE_VARINT);
                        picosOfMicro = reader.readVarint32();
                        break;
                    case TIMESTAMP_PRECISION:
                        expectWireType(tag, WIRETYPE_VARINT);
                        precision = reader.readVarint32();
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            checkRange("picos of micro", picosOfMicro, 0, PICOSECONDS_PER_MICROSECOND - 1);
            return ProtobufValueFormatter.formatTimestamp(checkPrecision(precision), epochMicros, picosOfMicro);
        }
    }

    private static class TimestampWithTimeZoneDecoder
            extends MessageDecoder
    {
        // The zone id of the last value: a column usually holds one zone, so the text is decoded
        // once. The entry is immutable and the field is only a hint, so a racing thread sees either
        // a complete entry or none
        private ZoneId lastZoneId;

        TimestampWithTimeZoneDecoder()
        {
            super(VALUE_TIMESTAMP_WITH_TIME_ZONE);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            long epochMillis = 0;
            int picosOfMilli = 0;
            int precision = 0;
            ZoneId zoneId = null;
            int offsetSeconds = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case TIMESTAMP_WITH_TIME_ZONE_EPOCH_MILLIS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        epochMillis = reader.readSint64();
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE_PICOS_OF_MILLI:
                        expectWireType(tag, WIRETYPE_VARINT);
                        picosOfMilli = reader.readVarint32();
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE_PRECISION:
                        expectWireType(tag, WIRETYPE_VARINT);
                        precision = reader.readVarint32();
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE_ZONE_ID:
                        expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                        zoneId = readZoneId(reader);
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE_OFFSET_SECONDS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        offsetSeconds = reader.readSint32();
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            if (zoneId == null) {
                throw new ProtobufFormatException("Timestamp with time zone has no zone id");
            }
            checkRange("picos of milli", picosOfMilli, 0, PICOSECONDS_PER_MILLISECOND - 1);
            checkRange("offset seconds", offsetSeconds, -MAX_OFFSET_SECONDS, MAX_OFFSET_SECONDS);
            // The server resolves the offset with its own time zone rules, so the client renders the
            // local date-time the json encoding would have, even for a zone the client does not know
            return ProtobufValueFormatter.formatTimestampWithTimeZone(checkPrecision(precision), epochMillis, picosOfMilli, offsetSeconds, zoneId.id, zoneId.ascii);
        }

        private ZoneId readZoneId(ProtobufWireReader reader)
        {
            int length = reader.readLength();
            ZoneId cached = lastZoneId;
            if (cached != null && reader.skipIfEqual(length, cached.utf8)) {
                return cached;
            }
            ZoneId zoneId = new ZoneId(reader.readBytes(length));
            lastZoneId = zoneId;
            return zoneId;
        }
    }

    private static final class ZoneId
    {
        private final byte[] utf8;
        private final String id;
        // the bytes again when they are all ASCII, which every zone id Trino knows is, otherwise null
        private final byte[] ascii;

        private ZoneId(byte[] utf8)
        {
            this.utf8 = utf8;
            this.id = new String(utf8, UTF_8);
            this.ascii = isAscii(utf8) ? utf8 : null;
        }

        private static boolean isAscii(byte[] bytes)
        {
            for (byte b : bytes) {
                if (b < 0) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class IntervalDayToSecondDecoder
            extends MessageDecoder
    {
        IntervalDayToSecondDecoder()
        {
            super(VALUE_INTERVAL_DAY_TO_SECOND);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            long micros = 0;
            int picosOfMicro = 0;
            int fractionalPrecision = 0;
            boolean hasMicros = false;
            long legacyMillis = 0;
            boolean hasLegacyMillis = false;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case INTERVAL_DAY_TO_SECOND_MICROS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        micros = reader.readSint64();
                        hasMicros = true;
                        break;
                    case INTERVAL_DAY_TO_SECOND_PICOS_OF_MICRO:
                        expectWireType(tag, WIRETYPE_VARINT);
                        picosOfMicro = reader.readVarint32();
                        hasMicros = true;
                        break;
                    case INTERVAL_DAY_TO_SECOND_FRACTIONAL_PRECISION:
                        expectWireType(tag, WIRETYPE_VARINT);
                        fractionalPrecision = reader.readVarint32();
                        hasMicros = true;
                        break;
                    case INTERVAL_DAY_TO_SECOND_LEGACY_MILLIS:
                        expectWireType(tag, WIRETYPE_VARINT);
                        legacyMillis = reader.readSint64();
                        hasLegacyMillis = true;
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            if (hasLegacyMillis) {
                if (hasMicros) {
                    throw new ProtobufFormatException("Interval has both legacy milliseconds and microseconds");
                }
                return IntervalDayTime.formatMillis(legacyMillis);
            }
            checkRange("picos of micro", picosOfMicro, 0, PICOSECONDS_PER_MICROSECOND - 1);
            return IntervalDayTime.formatInterval(micros, picosOfMicro, checkPrecision(fractionalPrecision));
        }
    }

    private static class NumberDecoder
            extends MessageDecoder
    {
        NumberDecoder()
        {
            super(VALUE_NUMBER);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            String value = null;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case NUMBER_DECIMAL:
                        expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                        int previousLimit = reader.pushLimit(reader.readLength());
                        // NUMBER renders through BigDecimal, which switches to scientific notation for small values
                        value = readBigDecimal(reader).toString();
                        reader.popLimit(previousLimit);
                        break;
                    case NUMBER_SPECIAL:
                        expectWireType(tag, WIRETYPE_VARINT);
                        int special = reader.readVarint32();
                        switch (special) {
                            case NUMBER_SPECIAL_NOT_A_NUMBER:
                                value = "NaN";
                                break;
                            case NUMBER_SPECIAL_POSITIVE_INFINITY:
                                value = "+Infinity";
                                break;
                            case NUMBER_SPECIAL_NEGATIVE_INFINITY:
                                value = "-Infinity";
                                break;
                            default:
                                throw new ProtobufFormatException("Unknown special number: " + special);
                        }
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            if (value == null) {
                throw new ProtobufFormatException("Number has no value");
            }
            return value;
        }
    }

    private static class VariantBinaryDecoder
            extends MessageDecoder
    {
        VariantBinaryDecoder()
        {
            super(VALUE_VARIANT);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            byte[] metadata = null;
            byte[] value = null;
            int tag = reader.readTag();
            expectKind(tag, VARIANT_BINARY, WIRETYPE_LENGTH_DELIMITED);
            int previousLimit = reader.pushLimit(reader.readLength());
            while (reader.hasRemaining()) {
                int fieldTag = reader.readTag();
                switch (fieldNumber(fieldTag)) {
                    case VARIANT_BINARY_METADATA:
                        expectWireType(fieldTag, WIRETYPE_LENGTH_DELIMITED);
                        metadata = reader.readBytes(reader.readLength());
                        break;
                    case VARIANT_BINARY_VALUE:
                        expectWireType(fieldTag, WIRETYPE_LENGTH_DELIMITED);
                        value = reader.readBytes(reader.readLength());
                        break;
                    default:
                        reader.skipField(fieldTag);
                }
            }
            reader.popLimit(previousLimit);
            if (metadata == null || value == null) {
                throw new ProtobufFormatException("Variant is missing metadata or value");
            }
            return EncodedVariant.fromBytes(metadata, value);
        }
    }

    private static class VariantJsonDecoder
            extends MessageDecoder
    {
        VariantJsonDecoder()
        {
            super(VALUE_VARIANT);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            int tag = reader.readTag();
            expectKind(tag, VARIANT_JSON, WIRETYPE_LENGTH_DELIMITED);
            String json = reader.readString(reader.readLength());
            // The json encoding sends the variant as a raw JSON value, so a variant null is
            // indistinguishable from SQL NULL there and decodes to null; match it
            if (json.equals("null")) {
                return null;
            }
            return json;
        }
    }

    private static class ArrayDecoder
            extends MessageDecoder
    {
        private final ValueDecoder elementDecoder;

        ArrayDecoder(ClientTypeSignature signature, boolean supportsVariantBinary)
        {
            super(VALUE_ARRAY);
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.elementDecoder = createValueDecoder(signature.getArgumentsAsTypeSignatures().get(0), supportsVariantBinary);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            List<Object> values = new ArrayList<>();
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                if (fieldNumber(tag) != ARRAY_ELEMENTS) {
                    reader.skipField(tag);
                    continue;
                }
                expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                values.add(readValue(reader, elementDecoder));
            }
            return unmodifiableList(values);
        }
    }

    private static class MapDecoder
            extends MessageDecoder
    {
        private final ValueDecoder keyDecoder;
        private final ValueDecoder valueDecoder;

        MapDecoder(ClientTypeSignature signature, boolean supportsVariantBinary)
        {
            super(VALUE_MAP);
            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = createValueDecoder(signature.getArgumentsAsTypeSignatures().get(0), supportsVariantBinary);
            this.valueDecoder = createValueDecoder(signature.getArgumentsAsTypeSignatures().get(1), supportsVariantBinary);
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            List<Object> keys = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case MAP_KEYS:
                        expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                        keys.add(readValue(reader, keyDecoder));
                        break;
                    case MAP_VALUES:
                        expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                        values.add(readValue(reader, valueDecoder));
                        break;
                    default:
                        reader.skipField(tag);
                }
            }
            if (keys.size() != values.size()) {
                throw new ProtobufFormatException(format("Map has %s keys but %s values", keys.size(), values.size()));
            }
            Map<Object, Object> map = new HashMap<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                map.put(keys.get(i), values.get(i));
                // the server never writes two equal keys, so a duplicate is a corrupt segment, not an entry to drop
                if (map.size() != i + 1) {
                    throw new ProtobufFormatException("Map has duplicate key: " + keys.get(i));
                }
            }
            return unmodifiableMap(map);
        }
    }

    private static class RowDecoder
            extends MessageDecoder
    {
        private final ValueDecoder[] fieldDecoders;
        private final List<Optional<String>> fieldNames;

        RowDecoder(ClientTypeSignature signature, boolean supportsVariantBinary)
        {
            super(VALUE_ROW);
            checkArgument(signature.getRawType().equals(ROW), "not a row type signature: %s", signature);
            fieldDecoders = new ValueDecoder[signature.getArguments().size()];
            ImmutableList.Builder<Optional<String>> fieldNames = ImmutableList.builderWithExpectedSize(fieldDecoders.length);
            int index = 0;
            for (ClientTypeSignatureParameter parameter : signature.getArguments()) {
                fieldDecoders[index] = createValueDecoder(parameter.getTypeSignature(), supportsVariantBinary);
                fieldNames.add(parameter.getName());
                index++;
            }
            this.fieldNames = fieldNames.build();
        }

        @Override
        Object decodeMessage(ProtobufWireReader reader)
        {
            Row.Builder row = Row.builderWithExpectedSize(fieldDecoders.length);
            int index = 0;
            while (reader.hasRemaining()) {
                int tag = reader.readTag();
                if (fieldNumber(tag) != ROW_VALUES) {
                    reader.skipField(tag);
                    continue;
                }
                expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
                if (index >= fieldDecoders.length) {
                    throw new ProtobufFormatException(format("Row has more than %s fields", fieldDecoders.length));
                }
                row.addField(fieldNames.get(index), readValue(reader, fieldDecoders[index]));
                index++;
            }
            if (index != fieldDecoders.length) {
                throw new ProtobufFormatException(format("Row has %s fields but %s were expected", index, fieldDecoders.length));
            }
            return row.build();
        }
    }

    /**
     * Types without a dedicated field are sent as bytes, text or a JSON document, whichever the
     * server could produce; the field itself says which.
     */
    private static class OtherTypeDecoder
            implements ValueDecoder
    {
        private final JsonMapper jsonMapper = new JsonMapper();

        @Override
        public Object decode(ProtobufWireReader reader)
        {
            int tag = readKind(reader);
            if (tag == -1) {
                return null;
            }
            expectWireType(tag, WIRETYPE_LENGTH_DELIMITED);
            switch (fieldNumber(tag)) {
                case VALUE_VARBINARY:
                    return reader.readBytes(reader.readLength());
                case VALUE_TEXT:
                    return reader.readString(reader.readLength());
                case VALUE_JSON_OBJECT:
                    try {
                        return jsonMapper.readValue(reader.readString(reader.readLength()), Object.class);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                default:
                    throw new ProtobufFormatException(format("Unexpected value field %s for a type without a dedicated encoding", fieldNumber(tag)));
            }
        }
    }

    private static void checkRange(String name, long value, long min, long max)
    {
        if (value < min || value > max) {
            throw new ProtobufFormatException(format("Invalid %s: %s", name, value));
        }
    }

    private static int checkPrecision(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new ProtobufFormatException("Invalid precision: " + precision);
        }
        return precision;
    }
}
