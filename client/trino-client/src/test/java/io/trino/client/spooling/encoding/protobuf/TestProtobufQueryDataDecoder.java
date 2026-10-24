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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.EncodedVariant;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.QueryDataDecoder;
import io.trino.client.Row;
import io.trino.client.RowFieldName;
import io.trino.client.spooling.DataAttribute;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import io.trino.client.spooling.encoding.protobuf.generated.ArrayValue;
import io.trino.client.spooling.encoding.protobuf.generated.Decimal;
import io.trino.client.spooling.encoding.protobuf.generated.IntervalDayToSecond;
import io.trino.client.spooling.encoding.protobuf.generated.MapValue;
import io.trino.client.spooling.encoding.protobuf.generated.NullValue;
import io.trino.client.spooling.encoding.protobuf.generated.NumberValue;
import io.trino.client.spooling.encoding.protobuf.generated.Segment;
import io.trino.client.spooling.encoding.protobuf.generated.Time;
import io.trino.client.spooling.encoding.protobuf.generated.TimeWithTimeZone;
import io.trino.client.spooling.encoding.protobuf.generated.Timestamp;
import io.trino.client.spooling.encoding.protobuf.generated.TimestampWithTimeZone;
import io.trino.client.spooling.encoding.protobuf.generated.Value;
import io.trino.client.spooling.encoding.protobuf.generated.VariantBinary;
import io.trino.client.spooling.encoding.protobuf.generated.VariantValue;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.GEOMETRY;
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
import static io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Decodes segments built with the protoc generated classes, so the hand-written decoder is checked
 * against the reference implementation and against spelled out expected values.
 */
public class TestProtobufQueryDataDecoder
{
    private static final Value NULL = Value.newBuilder().setNull(NullValue.NULL_VALUE).build();

    @Test
    public void testEncodingsAreRegistered()
    {
        assertThat(QueryDataDecoders.getSupportedEncodings()).contains("protobuf", "protobuf+zstd", "protobuf+lz4");
        for (String encoding : ImmutableList.of("protobuf", "protobuf+zstd", "protobuf+lz4")) {
            assertThat(QueryDataDecoders.get(encoding).encoding()).isEqualTo(encoding);
        }
        // The json encodings stay preferred until protobuf is the default
        assertThat(QueryDataDecoders.getPreferredEncodings()).isEqualTo("json+zstd,json+lz4,json,protobuf+zstd,protobuf+lz4,protobuf");
    }

    @Test
    public void testScalars()
    {
        assertDecodes(column(BOOLEAN), Value.newBuilder().setBoolean(true), true);
        assertDecodes(column(BOOLEAN), Value.newBuilder().setBoolean(false), false);
        assertDecodes(column(TINYINT), Value.newBuilder().setTinyint(-128), (byte) -128);
        assertDecodes(column(TINYINT), Value.newBuilder().setTinyint(127), (byte) 127);
        assertDecodes(column(SMALLINT), Value.newBuilder().setSmallint(-32768), (short) -32768);
        assertDecodes(column(INTEGER), Value.newBuilder().setInteger(Integer.MIN_VALUE), Integer.MIN_VALUE);
        assertDecodes(column(INTEGER), Value.newBuilder().setInteger(2137), 2137);
        assertDecodes(column(BIGINT), Value.newBuilder().setBigint(Long.MIN_VALUE), Long.MIN_VALUE);
        assertDecodes(column(BIGINT), Value.newBuilder().setBigint(Long.MAX_VALUE), Long.MAX_VALUE);
        assertDecodes(column(REAL), Value.newBuilder().setReal(1.5f), 1.5f);
        assertDecodes(column(REAL), Value.newBuilder().setReal(Float.NaN), Float.NaN);
        assertDecodes(column(DOUBLE), Value.newBuilder().setDouble(-0.0), -0.0);
        assertDecodes(column(DOUBLE), Value.newBuilder().setDouble(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
        assertDecodes(column(varchar()), Value.newBuilder().setVarchar("zażółć gęślą jaźń"), "zażółć gęślą jaźń");
        assertDecodes(column(varchar()), Value.newBuilder().setVarchar(""), "");
        assertDecodes(column(JSON), Value.newBuilder().setJson("{\"a\":[1,null]}"), "{\"a\":[1,null]}");
        assertDecodes(column(VARBINARY), Value.newBuilder().setVarbinary(ByteString.copyFrom(new byte[] {1, 2, (byte) 0xFF})), new byte[] {1, 2, (byte) 0xFF});
        assertDecodes(column(VARBINARY), Value.newBuilder().setVarbinary(ByteString.EMPTY), new byte[0]);
        for (ClientTypeSignature type : ImmutableList.of(new ClientTypeSignature(BOOLEAN), new ClientTypeSignature(TINYINT), new ClientTypeSignature(BIGINT), varchar(), new ClientTypeSignature(DATE), new ClientTypeSignature(TIMESTAMP), array(new ClientTypeSignature(BIGINT)))) {
            assertDecodes(column(type), NULL.toBuilder(), null);
        }
    }

    @Test
    public void testOutOfRangeIntegers()
    {
        assertThatThrownBy(() -> decode(column(TINYINT), Value.newBuilder().setTinyint(200)))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Tinyint out of range: 200");
        assertThatThrownBy(() -> decode(column(SMALLINT), Value.newBuilder().setSmallint(-40_000)))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Smallint out of range: -40000");
    }

    @Test
    public void testChar()
    {
        Column char5 = column(new ClientTypeSignature(CHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(5))));
        assertDecodes(char5, Value.newBuilder().setChar("abc"), "abc  ");
        assertDecodes(char5, Value.newBuilder().setChar("abcde"), "abcde");
        assertDecodes(char5, Value.newBuilder().setChar(""), "     ");
        // code points, not chars
        assertDecodes(char5, Value.newBuilder().setChar("zażó"), "zażó ");
        assertDecodes(char5, Value.newBuilder().setChar("🦃"), "🦃    ");
        // char without a length is char(1)
        assertDecodes(column(CHAR), Value.newBuilder().setChar(""), " ");
        assertDecodes(column(CHAR), Value.newBuilder().setChar("x"), "x");
    }

    @Test
    public void testDecimal()
    {
        Column decimal = column(DECIMAL);
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(12345).setScale(2)), "123.45");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(-12345).setScale(2)), "-123.45");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(5).setScale(3)), "0.005");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(-5).setScale(3)), "-0.005");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(0).setScale(2)), "0.00");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(0).setScale(0)), "0");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(123).setScale(0)), "123");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(100).setScale(2)), "1.00");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(Long.MIN_VALUE).setScale(0)), "-9223372036854775808");
        BigInteger big = BigInteger.TEN.pow(30);
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledBytes(ByteString.copyFrom(big.toByteArray())).setScale(10)), "100000000000000000000.0000000000");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledBytes(ByteString.copyFrom(big.negate().toByteArray())).setScale(31)), "-0.1000000000000000000000000000000");
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledBytes(ByteString.copyFrom(new BigInteger("9".repeat(38)).toByteArray())).setScale(0)), "9".repeat(38));
        // a small value may also come as bytes
        assertDecodes(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledBytes(ByteString.copyFrom(BigInteger.valueOf(-1).toByteArray())).setScale(1)), "-0.1");

        assertThatThrownBy(() -> decode(decimal, Value.newBuilder().setDecimal(Decimal.newBuilder().setScale(2))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Decimal has no unscaled value");
    }

    @Test
    public void testNumber()
    {
        Column number = column(NUMBER);
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(15).setScale(1))), "1.5");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(1).setScale(-10))), "1E+10");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(1).setScale(10))), "1E-10");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(0).setScale(0))), "0");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setSpecial(NumberValue.Special.NOT_A_NUMBER)), "NaN");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setSpecial(NumberValue.Special.POSITIVE_INFINITY)), "+Infinity");
        assertDecodes(number, Value.newBuilder().setNumber(NumberValue.newBuilder().setSpecial(NumberValue.Special.NEGATIVE_INFINITY)), "-Infinity");
        assertThatThrownBy(() -> decode(number, Value.newBuilder().setNumber(NumberValue.newBuilder())))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Number has no value");
    }

    @Test
    public void testDate()
    {
        assertDecodes(column(DATE), Value.newBuilder().setDate(0), "1970-01-01");
        assertDecodes(column(DATE), Value.newBuilder().setDate(18262), "2020-01-01");
        assertDecodes(column(DATE), Value.newBuilder().setDate(-1), "1969-12-31");
        assertDecodes(column(DATE), Value.newBuilder().setDate(-719528), "0000-01-01");
        assertDecodes(column(DATE), Value.newBuilder().setDate(2932896), "9999-12-31");
        assertDecodes(column(DATE), Value.newBuilder().setDate(2932897), "+10000-01-01");
    }

    @Test
    public void testTime()
    {
        long picos = 45_296_123_456_789_012L; // 12:34:56.123456789012
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(picos).setPrecision(12)), "12:34:56.123456789012");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(picos).setPrecision(9)), "12:34:56.123456789");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(45_296_123_000_000_000L).setPrecision(3)), "12:34:56.123");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(45_296_000_000_000_000L).setPrecision(0)), "12:34:56");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(0).setPrecision(6)), "00:00:00.000000");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(86_399_999_999_999_999L).setPrecision(12)), "23:59:59.999999999999");
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(1_000_000_000L).setPrecision(3)), "00:00:00.001");

        Column timeWithTimeZone = column(TIME_WITH_TIME_ZONE);
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(12).setOffsetMinutes(0)), "12:34:56.123456789012+00:00");
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(3).setOffsetMinutes(5 * 60 + 30)), "12:34:56.123+05:30");
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(0).setOffsetMinutes(-8 * 60)), "12:34:56-08:00");
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(0).setOffsetMinutes(-14 * 60)), "12:34:56-14:00");
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(0).setOffsetMinutes(-30)), "12:34:56-00:30");
        assertDecodes(timeWithTimeZone, Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picos).setPrecision(0).setOffsetMinutes(14 * 60)), "12:34:56+14:00");

        assertThatThrownBy(() -> decode(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(0).setPrecision(13))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid precision: 13");
    }

    @Test
    public void testTimestamp()
    {
        long epochMicros = 1_600_000_000_123_456L; // 2020-09-13 12:26:40.123456 UTC
        Column timestamp = column(TIMESTAMP);
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(epochMicros).setPrecision(6)), "2020-09-13 12:26:40.123456");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(epochMicros).setPicosOfMicro(789_012).setPrecision(12)), "2020-09-13 12:26:40.123456789012");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(epochMicros).setPicosOfMicro(789_000).setPrecision(9)), "2020-09-13 12:26:40.123456789");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(1_600_000_000_123_000L).setPrecision(3)), "2020-09-13 12:26:40.123");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(1_600_000_000_000_000L).setPrecision(0)), "2020-09-13 12:26:40");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(-1).setPrecision(6)), "1969-12-31 23:59:59.999999");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(-62_135_596_800_000_000L).setPrecision(0)), "0001-01-01 00:00:00");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(253_402_300_800_000_000L).setPrecision(0)), "+10000-01-01 00:00:00");
        assertDecodes(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(-62_167_219_200_000_000L - 1).setPrecision(6)), "-0001-12-31 23:59:59.999999");

        Column timestampWithTimeZone = column(TIMESTAMP_WITH_TIME_ZONE);
        long epochMillis = 1_600_000_000_123L;
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPrecision(3).setZoneId("UTC")), "2020-09-13 12:26:40.123 UTC");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPrecision(3).setZoneId("Europe/Warsaw").setOffsetSeconds(2 * 3600)), "2020-09-13 14:26:40.123 Europe/Warsaw");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPrecision(3).setZoneId("+05:30").setOffsetSeconds(5 * 3600 + 30 * 60)), "2020-09-13 17:56:40.123 +05:30");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPrecision(3).setZoneId("America/Los_Angeles").setOffsetSeconds(-7 * 3600)), "2020-09-13 05:26:40.123 America/Los_Angeles");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPicosOfMilli(456_789_012).setPrecision(12).setZoneId("UTC")), "2020-09-13 12:26:40.123456789012 UTC");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(1_600_000_000_000L).setPrecision(0).setZoneId("UTC")), "2020-09-13 12:26:40 UTC");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(-1).setPrecision(3).setZoneId("UTC")), "1969-12-31 23:59:59.999 UTC");

        // The local date-time comes from the offset the server resolved, not from the client's time zone rules:
        // a zone the client does not know, and a historical offset with seconds (local mean time of Warsaw in 1880)
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(epochMillis).setPrecision(3).setZoneId("Mars/Olympus_Mons").setOffsetSeconds(3600)), "2020-09-13 13:26:40.123 Mars/Olympus_Mons");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(-2_840_145_840_000L).setPrecision(0).setZoneId("Europe/Warsaw").setOffsetSeconds(5040)), "1880-01-01 00:00:00 Europe/Warsaw");
        assertDecodes(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(-1).setPicosOfMilli(999_999_999).setPrecision(12).setZoneId("-18:00").setOffsetSeconds(-18 * 3600)), "1969-12-31 05:59:59.999999999999 -18:00");

        assertThatThrownBy(() -> decode(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(0).setPrecision(3))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Timestamp with time zone has no zone id");
        assertThatThrownBy(() -> decode(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(0).setPrecision(3).setZoneId("UTC").setOffsetSeconds(18 * 3600 + 1))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid offset seconds: 64801");
        assertThatThrownBy(() -> decode(timestampWithTimeZone, Value.newBuilder().setTimestampWithTimeZone(TimestampWithTimeZone.newBuilder().setEpochMillis(0).setPicosOfMilli(1_000_000_000).setPrecision(12).setZoneId("UTC"))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos of milli: 1000000000");
        assertThatThrownBy(() -> decode(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(0).setPicosOfMicro(1_000_000).setPrecision(12))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos of micro: 1000000");
        assertThatThrownBy(() -> decode(timestamp, Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(0).setPicosOfMicro(-1).setPrecision(12))))
                .isInstanceOf(ProtobufFormatException.class);
    }

    @Test
    public void testIntervals()
    {
        Column yearToMonth = column(INTERVAL_YEAR_TO_MONTH);
        assertDecodes(yearToMonth, Value.newBuilder().setIntervalYearToMonth(14), "1-2");
        assertDecodes(yearToMonth, Value.newBuilder().setIntervalYearToMonth(-14), "-1-2");
        assertDecodes(yearToMonth, Value.newBuilder().setIntervalYearToMonth(0), "0-0");
        assertDecodes(yearToMonth, Value.newBuilder().setIntervalYearToMonth(Integer.MIN_VALUE), "-178956970-8");

        Column dayToSecond = column(INTERVAL_DAY_TO_SECOND);
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(93_784_000_000L).setFractionalPrecision(3)), "1 02:03:04.000");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(93_784_000_000L).setFractionalPrecision(0)), "1 02:03:04");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(1_234_567L).setFractionalPrecision(6)), "0 00:00:01.234567");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(-1_234_567L).setFractionalPrecision(6)), "-0 00:00:01.234567");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(1_234_567L).setPicosOfMicro(890_123).setFractionalPrecision(12)), "0 00:00:01.234567890123");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(-1_234_568L).setPicosOfMicro(109_877).setFractionalPrecision(12)), "-0 00:00:01.234567890123");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(0).setFractionalPrecision(6)), "0 00:00:00.000000");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(Long.MIN_VALUE).setFractionalPrecision(6)), "-106751991 04:00:54.775808");

        // the legacy millisecond form, which reaches past the microsecond range once rounded
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(93_784_005L)), "1 02:03:04.005");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(-1)), "-0 00:00:00.001");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(0)), "0 00:00:00.000");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(9_223_372_036_854_776L)), "106751991 04:00:54.776");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(-9_223_372_036_854_776L)), "-106751991 04:00:54.776");
        assertDecodes(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(Long.MIN_VALUE)), "-106751991167 07:12:55.808");
        assertThatThrownBy(() -> decode(dayToSecond, Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setLegacyMillis(1).setMicros(1_000).setFractionalPrecision(3))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Interval has both legacy milliseconds and microseconds");
    }

    @Test
    public void testUuidAndIpAddress()
    {
        byte[] uuid = new byte[] {0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};
        assertDecodes(column(UUID), Value.newBuilder().setUuid(ByteString.copyFrom(uuid)), "12345678-1234-5678-1234-567812345678");
        assertDecodes(column(UUID), Value.newBuilder().setUuid(ByteString.copyFrom(new byte[16])), "00000000-0000-0000-0000-000000000000");
        assertThatThrownBy(() -> decode(column(UUID), Value.newBuilder().setUuid(ByteString.copyFrom(new byte[15]))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected 16 bytes for uuid but got 15");

        byte[] ipv4 = new byte[16];
        ipv4[10] = (byte) 0xFF;
        ipv4[11] = (byte) 0xFF;
        ipv4[12] = 1;
        ipv4[13] = 2;
        ipv4[14] = 3;
        ipv4[15] = 4;
        assertDecodes(column(IPADDRESS), Value.newBuilder().setIpaddress(ByteString.copyFrom(ipv4)), "1.2.3.4");
        byte[] ipv6 = new byte[16];
        ipv6[0] = 0x20;
        ipv6[1] = 0x01;
        ipv6[2] = 0x0D;
        ipv6[3] = (byte) 0xB8;
        ipv6[15] = 1;
        assertDecodes(column(IPADDRESS), Value.newBuilder().setIpaddress(ByteString.copyFrom(ipv6)), "2001:db8::1");
        assertDecodes(column(IPADDRESS), Value.newBuilder().setIpaddress(ByteString.copyFrom(new byte[16])), "::");
        assertThatThrownBy(() -> decode(column(IPADDRESS), Value.newBuilder().setIpaddress(ByteString.copyFrom(new byte[4]))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected 16 bytes for ipaddress but got 4");
    }

    @Test
    public void testVariant()
    {
        byte[] metadata = {1, 0, 0};
        byte[] value = {12, 42};
        Value.Builder binary = Value.newBuilder().setVariant(VariantValue.newBuilder().setBinary(VariantBinary.newBuilder()
                .setMetadata(ByteString.copyFrom(metadata))
                .setValue(ByteString.copyFrom(value))));
        Object decoded = decode(column(VARIANT), binary, true);
        assertThat(decoded).isInstanceOf(EncodedVariant.class);
        assertThat(((EncodedVariant) decoded).getMetadataBytes()).isEqualTo(metadata);
        assertThat(((EncodedVariant) decoded).getValueBytes()).isEqualTo(value);
        // a binary variant for a client that does not support it
        assertThatThrownBy(() -> decode(column(VARIANT), binary, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Expected value field 2 but got 1");

        Value.Builder json = Value.newBuilder().setVariant(VariantValue.newBuilder().setJson("{\"a\":1}"));
        assertThat(decode(column(VARIANT), json, false)).isEqualTo("{\"a\":1}");
        // a variant null decodes to null like it does from the json encoding
        assertThat(decode(column(VARIANT), Value.newBuilder().setVariant(VariantValue.newBuilder().setJson("null")), false)).isNull();
        assertThatThrownBy(() -> decode(column(VARIANT), json, true))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Expected value field 1 but got 2");
        assertThatThrownBy(() -> decode(column(VARIANT), Value.newBuilder().setVariant(VariantValue.newBuilder().setBinary(VariantBinary.newBuilder().setMetadata(ByteString.copyFrom(metadata)))), true))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Variant is missing metadata or value");
    }

    @Test
    public void testArray()
    {
        Column arrayOfBigint = column(array(new ClientTypeSignature(BIGINT)));
        assertDecodes(
                arrayOfBigint,
                Value.newBuilder().setArray(ArrayValue.newBuilder()
                        .addElements(Value.newBuilder().setBigint(1))
                        .addElements(NULL)
                        .addElements(Value.newBuilder().setBigint(-3))),
                Arrays.asList(1L, null, -3L));
        assertDecodes(arrayOfBigint, Value.newBuilder().setArray(ArrayValue.newBuilder()), ImmutableList.of());
        assertDecodes(arrayOfBigint, NULL.toBuilder(), null);

        Column arrayOfArrayOfVarchar = column(array(array(varchar())));
        assertDecodes(
                arrayOfArrayOfVarchar,
                Value.newBuilder().setArray(ArrayValue.newBuilder()
                        .addElements(Value.newBuilder().setArray(ArrayValue.newBuilder().addElements(Value.newBuilder().setVarchar("a")).addElements(NULL)))
                        .addElements(NULL)
                        .addElements(Value.newBuilder().setArray(ArrayValue.newBuilder()))),
                Arrays.asList(Arrays.asList("a", null), null, ImmutableList.of()));

        assertThatThrownBy(() -> decode(arrayOfBigint, Value.newBuilder().setArray(ArrayValue.newBuilder().addElements(Value.newBuilder().setVarchar("x")))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected value field 6 but got 10");
    }

    @Test
    public void testMap()
    {
        Column varcharToBigint = column(map(varchar(), new ClientTypeSignature(BIGINT)));
        assertDecodes(
                varcharToBigint,
                Value.newBuilder().setMap(MapValue.newBuilder()
                        .addKeys(Value.newBuilder().setVarchar("a"))
                        .addKeys(Value.newBuilder().setVarchar("b"))
                        .addValues(Value.newBuilder().setBigint(1))
                        .addValues(NULL)),
                mapOf("a", 1L, "b", null));
        assertDecodes(varcharToBigint, Value.newBuilder().setMap(MapValue.newBuilder()), ImmutableMap.of());

        // keys are typed
        Column dateToDouble = column(map(new ClientTypeSignature(DATE), new ClientTypeSignature(DOUBLE)));
        assertDecodes(
                dateToDouble,
                Value.newBuilder().setMap(MapValue.newBuilder()
                        .addKeys(Value.newBuilder().setDate(18262))
                        .addValues(Value.newBuilder().setDouble(0.5))),
                ImmutableMap.of("2020-01-01", 0.5));
        Column bigintToArray = column(map(new ClientTypeSignature(BIGINT), array(varchar())));
        assertDecodes(
                bigintToArray,
                Value.newBuilder().setMap(MapValue.newBuilder()
                        .addKeys(Value.newBuilder().setBigint(7))
                        .addValues(Value.newBuilder().setArray(ArrayValue.newBuilder().addElements(Value.newBuilder().setVarchar("x"))))),
                ImmutableMap.of(7L, ImmutableList.of("x")));

        assertThatThrownBy(() -> decode(varcharToBigint, Value.newBuilder().setMap(MapValue.newBuilder()
                .addKeys(Value.newBuilder().setVarchar("a"))
                .addKeys(Value.newBuilder().setVarchar("b"))
                .addValues(Value.newBuilder().setBigint(1)))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Map has 2 keys but 1 values");
        // a duplicate key is a corrupt segment, not an entry to drop silently
        assertThatThrownBy(() -> decode(varcharToBigint, Value.newBuilder().setMap(MapValue.newBuilder()
                .addKeys(Value.newBuilder().setVarchar("a"))
                .addKeys(Value.newBuilder().setVarchar("a"))
                .addValues(Value.newBuilder().setBigint(1))
                .addValues(NULL))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Map has duplicate key: a");
        // a null value does not look like a duplicate
        assertDecodes(varcharToBigint, Value.newBuilder().setMap(MapValue.newBuilder()
                .addKeys(Value.newBuilder().setVarchar("a"))
                .addKeys(Value.newBuilder().setVarchar("b"))
                .addValues(NULL)
                .addValues(NULL)), nullValues("a", "b"));
    }

    private static Map<Object, Object> nullValues(Object... keys)
    {
        Map<Object, Object> map = new HashMap<>();
        for (Object key : keys) {
            map.put(key, null);
        }
        return map;
    }

    @Test
    public void testRow()
    {
        ClientTypeSignature rowSignature = new ClientTypeSignature(ROW, ImmutableList.of(
                ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("a")), new ClientTypeSignature(BIGINT))),
                ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.empty(), varchar())),
                ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("nested")), array(new ClientTypeSignature(BOOLEAN))))));
        Column row = column(rowSignature);
        Value.Builder value = Value.newBuilder().setRow(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder()
                .addValues(Value.newBuilder().setBigint(1))
                .addValues(NULL)
                .addValues(Value.newBuilder().setArray(ArrayValue.newBuilder().addElements(Value.newBuilder().setBoolean(true)))));
        assertDecodes(row, value, Row.builder()
                .addField("a", 1L)
                .addUnnamedField(null)
                .addField("nested", ImmutableList.of(true))
                .build());
        assertDecodes(row, NULL.toBuilder(), null);

        assertThatThrownBy(() -> decode(row, Value.newBuilder().setRow(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(Value.newBuilder().setBigint(1)))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Row has 1 fields but 3 were expected");
        assertThatThrownBy(() -> decode(row, value.clone().setRow(value.getRow().toBuilder().addValues(NULL))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Row has more than 3 fields");
    }

    @Test
    public void testTypesWithoutDedicatedEncoding()
    {
        Column geometry = column(GEOMETRY);
        assertDecodes(geometry, Value.newBuilder().setText("POINT (1 2)"), "POINT (1 2)");
        assertDecodes(geometry, Value.newBuilder().setVarbinary(ByteString.copyFrom(new byte[] {1})), new byte[] {1});
        assertDecodes(geometry, Value.newBuilder().setJsonObject("{\"x\":1,\"y\":[true]}"), ImmutableMap.of("x", 1, "y", ImmutableList.of(true)));
        assertDecodes(geometry, NULL.toBuilder(), null);
        assertThatThrownBy(() -> decode(geometry, Value.newBuilder().setVarchar("x")))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Unexpected value field 10 for a type without a dedicated encoding");
    }

    @Test
    public void testUnknownFieldsInMessagesAreSkipped()
    {
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(7).addFixed32(1).addFixed64(2).addLengthDelimited(ByteString.copyFromUtf8("x")).build())
                .build();
        // inside a nested message
        assertDecodes(column(TIMESTAMP), Value.newBuilder().setTimestamp(Timestamp.newBuilder().setEpochMicros(0).setPrecision(0).setUnknownFields(unknown)), "1970-01-01 00:00:00");
        assertDecodes(column(DECIMAL), Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(1).setScale(0).setUnknownFields(unknown)), "1");
        assertDecodes(column(array(new ClientTypeSignature(BIGINT))), Value.newBuilder().setArray(ArrayValue.newBuilder().addElements(Value.newBuilder().setBigint(1)).setUnknownFields(unknown)), ImmutableList.of(1L));
        // inside a row and a segment
        Segment segment = Segment.newBuilder()
                .addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(Value.newBuilder().setBigint(1)).setUnknownFields(unknown))
                .setUnknownFields(unknown)
                .build();
        assertThat(decode(ImmutableList.of(column(BIGINT)), segment.toByteArray(), false)).isEqualTo(ImmutableList.of(ImmutableList.of(1L)));
        // but not as the kind of a value
        assertThatThrownBy(() -> decode(column(BIGINT), Value.newBuilder().setUnknownFields(unknown)))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected value field 6 but got 99");
    }

    @Test
    public void testMalformedValues()
    {
        assertThatThrownBy(() -> decode(column(BIGINT), Value.newBuilder()))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Value has no kind set");
        assertThatThrownBy(() -> decode(column(BIGINT), Value.newBuilder().setVarchar("1")))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected value field 6 but got 10");
        // wrong wire type for the right field: bigint as length delimited
        byte[] bigintAsBytes = segment(bytes(0x32, 0x01, 0x01));
        assertThatThrownBy(() -> decode(ImmutableList.of(column(BIGINT)), bigintAsBytes, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected wire type 0 for field 6 but got 2");
        // two kinds set: null and bigint
        byte[] twoKinds = segment(bytes(0x08, 0x00, 0x30, 0x02));
        assertThatThrownBy(() -> decode(ImmutableList.of(column(BIGINT)), twoKinds, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Message was not fully consumed, 2 bytes remaining");
        // the null marker with a wrong wire type
        byte[] nullAsBytes = segment(bytes(0x0A, 0x00));
        assertThatThrownBy(() -> decode(ImmutableList.of(column(BIGINT)), nullAsBytes, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Expected wire type 0 for field 1 but got 2");
    }

    @Test
    public void testOutOfRangeTemporalValues()
    {
        long picosPerDay = 86_400_000_000_000_000L;
        assertDecodes(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(picosPerDay - 1).setPrecision(12)), "23:59:59.999999999999");
        assertThatThrownBy(() -> decode(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(picosPerDay).setPrecision(12))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos: 86400000000000000");
        assertThatThrownBy(() -> decode(column(TIME), Value.newBuilder().setTime(Time.newBuilder().setPicos(-1).setPrecision(12))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos: -1");
        assertThatThrownBy(() -> decode(column(TIME_WITH_TIME_ZONE), Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(picosPerDay).setPrecision(3))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos: 86400000000000000");
        assertDecodes(column(TIME_WITH_TIME_ZONE), Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(0).setPrecision(0).setOffsetMinutes(-14 * 60)), "00:00:00-14:00");
        assertThatThrownBy(() -> decode(column(TIME_WITH_TIME_ZONE), Value.newBuilder().setTimeWithTimeZone(TimeWithTimeZone.newBuilder().setPicos(0).setPrecision(0).setOffsetMinutes(14 * 60 + 1))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid offset minutes: 841");
        assertThatThrownBy(() -> decode(column(INTERVAL_DAY_TO_SECOND), Value.newBuilder().setIntervalDayToSecond(IntervalDayToSecond.newBuilder().setMicros(1).setPicosOfMicro(1_000_000).setFractionalPrecision(12))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid picos of micro: 1000000");
    }

    @Test
    public void testMalformedDecimals()
    {
        assertThatThrownBy(() -> decode(column(DECIMAL), Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(1).setScale(-1))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid decimal scale: -1");
        assertThatThrownBy(() -> decode(column(DECIMAL), Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledBytes(ByteString.EMPTY).setScale(2))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Decimal has empty unscaled bytes");
        assertThatThrownBy(() -> decode(column(DECIMAL), Value.newBuilder().setDecimal(Decimal.newBuilder().setScale(2))))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Decimal has no unscaled value");
        // NUMBER is rendered through BigDecimal, so a negative scale is a valid number
        assertDecodes(column(NUMBER), Value.newBuilder().setNumber(NumberValue.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(12).setScale(-3))), "1.2E+4");
    }

    @Test
    public void testMalformedSegments()
    {
        Segment segment = Segment.newBuilder()
                .addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(Value.newBuilder().setBigint(1)).addValues(Value.newBuilder().setVarchar("a")))
                .build();
        List<Column> columns = ImmutableList.of(column(BIGINT), column(varchar()));
        byte[] bytes = segment.toByteArray();
        assertThat(decode(columns, bytes, false)).isEqualTo(ImmutableList.of(ImmutableList.of(1L, "a")));

        // fewer or more columns than values
        assertThatThrownBy(() -> decode(ImmutableList.of(column(BIGINT)), bytes, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Row has more than 1 values");
        assertThatThrownBy(() -> decode(ImmutableList.of(column(BIGINT), column(varchar()), column(varchar())), bytes, false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Row has 2 values but 3 columns were expected");
        // truncated in the middle of a row
        for (int length = 1; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThatThrownBy(() -> decode(columns, truncated, false))
                    .isInstanceOfAny(UncheckedIOException.class, ProtobufFormatException.class);
        }
        // a row length larger than the segment
        assertThatThrownBy(() -> decode(columns, bytes(0x0A, 0x7F, 0x0A), false))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Truncated row: expected 127 bytes but got 1");
        // rows must be length delimited
        assertThatThrownBy(() -> decode(columns, bytes(0x08, 0x01), false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Unexpected wire type 0 for segment rows");
        // an empty segment has no rows
        assertThat(decode(columns, new byte[0], false)).isEmpty();
        // a corrupt row length close to 2 GB fails as truncated, without allocating a buffer of that size first
        assertThatThrownBy(() -> decode(columns, bytes(0x0A, 0xF7, 0xFF, 0xFF, 0xFF, 0x07, 0x0A, 0x02), false))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Truncated row: expected 2147483639 bytes but got 2");
        // field number 0, and a tag that does not fit in 32 bits
        assertThatThrownBy(() -> decode(columns, bytes(0x02, 0x00), false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid segment field tag: 2");
        assertThatThrownBy(() -> decode(columns, bytes(0x8A, 0x80, 0x80, 0x80, 0x10, 0x00), false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Invalid segment field tag: 4294967306");
        // an empty row is a row with no values, which is an error for a query with columns
        assertThatThrownBy(() -> decode(columns, bytes(0x0A, 0x00), false))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessage("Row has 0 values but 2 columns were expected");
    }

    @Test
    public void testIteratorContract()
            throws IOException
    {
        Segment segment = Segment.newBuilder()
                .addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(Value.newBuilder().setBigint(1)))
                .addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(Value.newBuilder().setBigint(2)))
                .build();
        boolean[] closed = new boolean[1];
        InputStream input = new ByteArrayInputStream(segment.toByteArray())
        {
            @Override
            public void close()
            {
                closed[0] = true;
            }
        };
        QueryDataDecoder decoder = QueryDataDecoders.get("protobuf").create(ImmutableList.of(column(BIGINT)), DataAttributes.empty(), false);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(input, DataAttributes.empty())) {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isEqualTo(singletonList(1L));
            assertThat(iterator.next()).isEqualTo(singletonList(2L));
            assertThat(closed[0]).isFalse();
            assertThat(iterator.hasNext()).isFalse();
            // the stream is closed once the segment is consumed
            assertThat(closed[0]).isTrue();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }

        closed[0] = false;
        InputStream unread = new ByteArrayInputStream(segment.toByteArray())
        {
            @Override
            public void close()
            {
                closed[0] = true;
            }
        };
        CloseableIterator<List<Object>> iterator = decoder.decode(unread, DataAttributes.empty());
        iterator.close();
        assertThat(closed[0]).isTrue();
    }

    @Test
    public void testCompressedEncodings()
            throws IOException
    {
        Segment.Builder segment = Segment.newBuilder();
        for (int i = 0; i < 1000; i++) {
            segment.addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder()
                    .addValues(Value.newBuilder().setBigint(i))
                    .addValues(Value.newBuilder().setVarchar("value " + i)));
        }
        byte[] uncompressed = segment.build().toByteArray();
        List<Column> columns = ImmutableList.of(column(BIGINT), column(varchar()));
        List<List<Object>> expected = decode(columns, uncompressed, false);
        assertThat(expected).hasSize(1000);
        assertThat(expected.get(999)).isEqualTo(ImmutableList.of(999L, "value 999"));

        ZstdCompressor zstd = ZstdCompressor.create();
        byte[] zstdBuffer = new byte[zstd.maxCompressedLength(uncompressed.length)];
        int zstdSize = zstd.compress(uncompressed, 0, uncompressed.length, zstdBuffer, 0, zstdBuffer.length);
        assertThat(decodeCompressed("protobuf+zstd", columns, Arrays.copyOf(zstdBuffer, zstdSize), uncompressed.length)).isEqualTo(expected);

        Lz4Compressor lz4 = Lz4Compressor.create();
        byte[] lz4Buffer = new byte[lz4.maxCompressedLength(uncompressed.length)];
        int lz4Size = lz4.compress(uncompressed, 0, uncompressed.length, lz4Buffer, 0, lz4Buffer.length);
        assertThat(decodeCompressed("protobuf+lz4", columns, Arrays.copyOf(lz4Buffer, lz4Size), uncompressed.length)).isEqualTo(expected);

        // a segment below the compression threshold is passed through uncompressed
        DataAttributes attributes = DataAttributes.builder().set(DataAttribute.SEGMENT_SIZE, uncompressed.length).build();
        QueryDataDecoder decoder = QueryDataDecoders.get("protobuf+zstd").create(columns, DataAttributes.empty(), false);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(uncompressed), attributes)) {
            assertThat(ImmutableList.copyOf(iterator)).isEqualTo(expected);
        }
    }

    private static List<List<Object>> decodeCompressed(String encoding, List<Column> columns, byte[] compressed, int uncompressedSize)
            throws IOException
    {
        DataAttributes attributes = DataAttributes.builder()
                .set(DataAttribute.SEGMENT_SIZE, compressed.length)
                .set(DataAttribute.UNCOMPRESSED_SIZE, uncompressedSize)
                .build();
        QueryDataDecoder decoder = QueryDataDecoders.get(encoding).create(columns, DataAttributes.empty(), false);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(compressed), attributes)) {
            return ImmutableList.copyOf(iterator);
        }
    }

    private static void assertDecodes(Column column, Value.Builder value, Object expected)
    {
        Object actual = decode(column, value);
        if (expected instanceof byte[]) {
            assertThat(actual).isInstanceOf(byte[].class);
            assertThat((byte[]) actual).isEqualTo((byte[]) expected);
        }
        else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static Object decode(Column column, Value.Builder value)
    {
        return decode(column, value, true);
    }

    private static Object decode(Column column, Value.Builder value, boolean supportsVariantBinary)
    {
        Segment segment = Segment.newBuilder()
                .addRows(io.trino.client.spooling.encoding.protobuf.generated.Row.newBuilder().addValues(value))
                .build();
        List<List<Object>> rows = decode(ImmutableList.of(column), segment.toByteArray(), supportsVariantBinary);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).hasSize(1);
        return rows.get(0).get(0);
    }

    private static List<List<Object>> decode(List<Column> columns, byte[] segment, boolean supportsVariantBinary)
    {
        QueryDataDecoder decoder = QueryDataDecoders.get("protobuf").create(columns, DataAttributes.empty(), supportsVariantBinary);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(segment), DataAttributes.empty())) {
            return ImmutableList.copyOf(iterator);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] segment(byte[] valueContent)
    {
        // Segment { rows: Row { values: Value { valueContent } } }
        byte[] result = new byte[valueContent.length + 4];
        result[0] = 0x0A;
        result[1] = (byte) (valueContent.length + 2);
        result[2] = 0x0A;
        result[3] = (byte) valueContent.length;
        System.arraycopy(valueContent, 0, result, 4, valueContent.length);
        return result;
    }

    private static byte[] bytes(int... values)
    {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private static Column column(String type)
    {
        return column(new ClientTypeSignature(type));
    }

    private static Column column(ClientTypeSignature signature)
    {
        return new Column("col", signature.toString(), signature);
    }

    private static ClientTypeSignature varchar()
    {
        // the server always sends a length argument for varchar
        return new ClientTypeSignature(VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)));
    }

    private static ClientTypeSignature array(ClientTypeSignature element)
    {
        return new ClientTypeSignature(ARRAY, ImmutableList.of(ClientTypeSignatureParameter.ofType(element)));
    }

    private static ClientTypeSignature map(ClientTypeSignature key, ClientTypeSignature value)
    {
        return new ClientTypeSignature(MAP, ImmutableList.of(ClientTypeSignatureParameter.ofType(key), ClientTypeSignatureParameter.ofType(value)));
    }

    private static Map<Object, Object> mapOf(Object key1, Object value1, Object key2, Object value2)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
