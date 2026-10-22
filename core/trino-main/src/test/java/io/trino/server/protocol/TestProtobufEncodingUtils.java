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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttribute;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncodingConfig;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ProtobufQueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.variant.Variant;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.LongInterval;
import io.trino.util.variant.VariantUtil;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.type.ColorType.COLOR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks that every type decodes from the protobuf encoding to exactly the Java values that the json
 * encoding produces for the same pages, including the client capability variants.
 */
public class TestProtobufEncodingUtils
{
    private static final List<String> PROTOBUF_ENCODINGS = ImmutableList.of("protobuf", "protobuf+zstd", "protobuf+lz4");

    private static final int COMPRESSION_THRESHOLD = 8 * 1024;

    @Test
    public void testBoolean()
            throws IOException
    {
        assertRoundTrip(BOOLEAN, builder -> {
            BOOLEAN.writeBoolean(builder, true);
            BOOLEAN.writeBoolean(builder, false);
            builder.appendNull();
        });
    }

    @Test
    public void testIntegers()
            throws IOException
    {
        assertRoundTrip(TINYINT, builder -> {
            TINYINT.writeLong(builder, Byte.MIN_VALUE);
            TINYINT.writeLong(builder, -1);
            TINYINT.writeLong(builder, 0);
            TINYINT.writeLong(builder, Byte.MAX_VALUE);
            builder.appendNull();
        });
        assertRoundTrip(SMALLINT, builder -> {
            SMALLINT.writeLong(builder, Short.MIN_VALUE);
            SMALLINT.writeLong(builder, -1);
            SMALLINT.writeLong(builder, 0);
            SMALLINT.writeLong(builder, Short.MAX_VALUE);
            builder.appendNull();
        });
        assertRoundTrip(INTEGER, builder -> {
            INTEGER.writeLong(builder, Integer.MIN_VALUE);
            INTEGER.writeLong(builder, -1);
            INTEGER.writeLong(builder, 0);
            INTEGER.writeLong(builder, 2137);
            INTEGER.writeLong(builder, Integer.MAX_VALUE);
            builder.appendNull();
        });
        assertRoundTrip(BIGINT, builder -> {
            BIGINT.writeLong(builder, Long.MIN_VALUE);
            BIGINT.writeLong(builder, -1);
            BIGINT.writeLong(builder, 0);
            BIGINT.writeLong(builder, 1L << 40);
            BIGINT.writeLong(builder, Long.MAX_VALUE);
            builder.appendNull();
        });
    }

    @Test
    public void testFloatingPoint()
            throws IOException
    {
        assertRoundTrip(REAL, builder -> {
            REAL.writeFloat(builder, 1.5f);
            REAL.writeFloat(builder, -0.0f);
            REAL.writeFloat(builder, Float.MIN_VALUE);
            REAL.writeFloat(builder, Float.MAX_VALUE);
            REAL.writeFloat(builder, Float.NaN);
            REAL.writeFloat(builder, Float.POSITIVE_INFINITY);
            REAL.writeFloat(builder, Float.NEGATIVE_INFINITY);
            builder.appendNull();
        });
        assertRoundTrip(DOUBLE, builder -> {
            DOUBLE.writeDouble(builder, 1.5);
            DOUBLE.writeDouble(builder, -0.0);
            DOUBLE.writeDouble(builder, Double.MIN_VALUE);
            DOUBLE.writeDouble(builder, Double.MAX_VALUE);
            DOUBLE.writeDouble(builder, Double.NaN);
            DOUBLE.writeDouble(builder, Double.POSITIVE_INFINITY);
            DOUBLE.writeDouble(builder, Double.NEGATIVE_INFINITY);
            builder.appendNull();
        });
    }

    @Test
    public void testDecimal()
            throws IOException
    {
        DecimalType shortDecimal = createDecimalType(5, 2);
        assertRoundTrip(shortDecimal, builder -> {
            shortDecimal.writeLong(builder, 12345);
            shortDecimal.writeLong(builder, -12345);
            shortDecimal.writeLong(builder, 5);
            shortDecimal.writeLong(builder, -5);
            shortDecimal.writeLong(builder, 0);
            builder.appendNull();
        });
        DecimalType integral = createDecimalType(18, 0);
        assertRoundTrip(integral, builder -> {
            integral.writeLong(builder, 999_999_999_999_999_999L);
            integral.writeLong(builder, -999_999_999_999_999_999L);
            integral.writeLong(builder, 0);
        });
        DecimalType longDecimal = createDecimalType(38, 10);
        assertRoundTrip(longDecimal, builder -> {
            longDecimal.writeObject(builder, Int128.valueOf(new BigInteger("9".repeat(38))));
            longDecimal.writeObject(builder, Int128.valueOf(new BigInteger("-" + "9".repeat(38))));
            longDecimal.writeObject(builder, Int128.valueOf(new BigInteger("12345678901234567890123456789")));
            // fits in 64 bits, so it goes as a varint
            longDecimal.writeObject(builder, Int128.valueOf(1234567890123L));
            longDecimal.writeObject(builder, Int128.valueOf(-1234567890123L));
            longDecimal.writeObject(builder, Int128.valueOf(Long.MIN_VALUE));
            longDecimal.writeObject(builder, Int128.valueOf(Long.MAX_VALUE));
            longDecimal.writeObject(builder, Int128.valueOf(new BigInteger("9223372036854775808"))); // Long.MAX_VALUE + 1
            longDecimal.writeObject(builder, Int128.valueOf(new BigInteger("-9223372036854775809"))); // Long.MIN_VALUE - 1
            longDecimal.writeObject(builder, Int128.valueOf(0));
            longDecimal.writeObject(builder, Int128.valueOf(7));
            builder.appendNull();
        });
    }

    @Test
    public void testStrings()
            throws IOException
    {
        assertRoundTrip(VARCHAR, builder -> {
            VARCHAR.writeSlice(builder, utf8Slice("ala ma kota"));
            VARCHAR.writeSlice(builder, utf8Slice(""));
            VARCHAR.writeSlice(builder, utf8Slice("数据应用"));
            VARCHAR.writeSlice(builder, utf8Slice("zażółć gęślą jaźń"));
            VARCHAR.writeSlice(builder, utf8Slice("\0\0\0"));
            VARCHAR.writeSlice(builder, utf8Slice("\r\t\n\"quoted\""));
            VARCHAR.writeSlice(builder, utf8Slice("🦃"));
            builder.appendNull();
        });
        Type varchar3 = createVarcharType(3);
        assertRoundTrip(varchar3, builder -> {
            varchar3.writeSlice(builder, utf8Slice("abc"));
            varchar3.writeSlice(builder, utf8Slice("a"));
        });
        Type char5 = createCharType(5);
        assertRoundTrip(char5, builder -> {
            // chars are stored without the trailing spaces
            char5.writeSlice(builder, utf8Slice("abc"));
            char5.writeSlice(builder, utf8Slice("abcde"));
            char5.writeSlice(builder, utf8Slice(""));
            char5.writeSlice(builder, utf8Slice("zażó"));
            char5.writeSlice(builder, utf8Slice("🦃"));
            builder.appendNull();
        });
        assertRoundTrip(JSON, builder -> {
            JSON.writeSlice(builder, utf8Slice("{\"a\":1,\"b\":[true,null,\"x\"]}"));
            JSON.writeSlice(builder, utf8Slice("null"));
            JSON.writeSlice(builder, utf8Slice("\"text\""));
            builder.appendNull();
        });
    }

    @Test
    public void testVarbinary()
            throws IOException
    {
        assertRoundTrip(VARBINARY, builder -> {
            VARBINARY.writeSlice(builder, wrappedBuffer(new byte[] {1, 2, 3, (byte) 0xFF}));
            VARBINARY.writeSlice(builder, wrappedBuffer(new byte[0]));
            VARBINARY.writeSlice(builder, wrappedBuffer(new byte[1000]));
            builder.appendNull();
        });
    }

    @Test
    public void testDate()
            throws IOException
    {
        assertRoundTrip(DATE, builder -> {
            DATE.writeLong(builder, 0);
            DATE.writeLong(builder, 18262); // 2020-01-01
            DATE.writeLong(builder, -719528); // 0000-01-01
            DATE.writeLong(builder, -1);
            DATE.writeLong(builder, 2932896); // 9999-12-31
            DATE.writeLong(builder, 2932897); // +10000-01-01
            DATE.writeLong(builder, -1000000);
            builder.appendNull();
        });
    }

    @Test
    public void testTime()
            throws IOException
    {
        for (int p = 0; p <= 12; p++) {
            int precision = p;
            Type type = createTimeType(precision);
            long unit = pow10(12 - precision);
            assertRoundTrip(type, builder -> {
                type.writeLong(builder, 0);
                type.writeLong(builder, 45_296_000_000_000_000L); // 12:34:56
                type.writeLong(builder, 45_296_000_000_000_000L + 123_456_789_012L / unit * unit);
                type.writeLong(builder, 86_399_999_999_999_999L / unit * unit); // 23:59:59.999999999999
                builder.appendNull();
            });
        }
    }

    @Test
    public void testTimeWithTimeZone()
            throws IOException
    {
        for (int p = 0; p <= 12; p++) {
            int precision = p;
            TimeWithTimeZoneType type = createTimeWithTimeZoneType(precision);
            long unit = pow10(12 - precision);
            long picos = 45_296_000_000_000_000L + 123_456_789_012L / unit * unit;
            assertRoundTrip(type, builder -> {
                if (type.isShort()) {
                    type.writeLong(builder, packTimeWithTimeZone(picos / 1000, 0));
                    type.writeLong(builder, packTimeWithTimeZone(picos / 1000, 5 * 60 + 30));
                    type.writeLong(builder, packTimeWithTimeZone(picos / 1000, -(8 * 60)));
                    type.writeLong(builder, packTimeWithTimeZone(picos / 1000, -(14 * 60)));
                    type.writeLong(builder, packTimeWithTimeZone(0, 14 * 60));
                }
                else {
                    type.writeObject(builder, new LongTimeWithTimeZone(picos, 0));
                    type.writeObject(builder, new LongTimeWithTimeZone(picos, 5 * 60 + 30));
                    type.writeObject(builder, new LongTimeWithTimeZone(picos, -(8 * 60)));
                    type.writeObject(builder, new LongTimeWithTimeZone(picos, -(14 * 60)));
                    type.writeObject(builder, new LongTimeWithTimeZone(0, 14 * 60));
                }
                builder.appendNull();
            });
        }
    }

    @Test
    public void testTimestamp()
            throws IOException
    {
        for (int p = 0; p <= 12; p++) {
            int precision = p;
            TimestampType type = createTimestampType(precision);
            assertRoundTrip(type, builder -> {
                for (long epochMicros : new long[] {0, 1_600_000_000_123_456L, -1_600_000_000_123_456L, -62_135_596_800_000_000L, 253_402_300_799_000_000L, 253_402_300_800_000_000L}) {
                    if (type.isShort()) {
                        long unit = pow10(6 - precision);
                        type.writeLong(builder, epochMicros / unit * unit);
                    }
                    else {
                        long unit = pow10(12 - precision);
                        type.writeObject(builder, new LongTimestamp(epochMicros, (int) (999_999 / unit * unit)));
                    }
                }
                builder.appendNull();
            });
        }
    }

    @Test
    public void testTimestampWithTimeZone()
            throws IOException
    {
        List<TimeZoneKey> zones = ImmutableList.of(UTC_KEY, getTimeZoneKey("Europe/Warsaw"), getTimeZoneKey("America/Los_Angeles"), getTimeZoneKey("+05:30"), getTimeZoneKey("Asia/Kathmandu"));
        for (int p = 0; p <= 12; p++) {
            int precision = p;
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            assertRoundTrip(type, builder -> {
                for (TimeZoneKey zone : zones) {
                    for (long epochMillis : new long[] {0, 1_600_000_000_123L, -1_600_000_000_123L, -62_135_596_800_000L, 253_402_300_799_000L}) {
                        if (type.isShort()) {
                            long unit = pow10(3 - precision);
                            type.writeLong(builder, packDateTimeWithZone(epochMillis / unit * unit, zone));
                        }
                        else {
                            long unit = pow10(12 - precision);
                            type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, (int) (999_999_999 / unit * unit), zone));
                        }
                    }
                }
                builder.appendNull();
            });
        }
    }

    @Test
    public void testIntervals()
            throws IOException
    {
        assertRoundTrip(INTERVAL_YEAR_MONTH, builder -> {
            INTERVAL_YEAR_MONTH.writeLong(builder, 0);
            INTERVAL_YEAR_MONTH.writeLong(builder, 14);
            INTERVAL_YEAR_MONTH.writeLong(builder, -14);
            INTERVAL_YEAR_MONTH.writeLong(builder, Integer.MAX_VALUE);
            INTERVAL_YEAR_MONTH.writeLong(builder, Integer.MIN_VALUE);
            builder.appendNull();
        });
        for (int p = 0; p <= 12; p++) {
            int precision = p;
            IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, 9, precision);
            assertRoundTrip(type, builder -> {
                for (long micros : new long[] {0, 1_234_567L, -1_234_567L, 93_784_000_000L, -93_784_000_000L, 1_234_567_890_123_456L, -1_234_567_890_123_456L}) {
                    if (type.isShort()) {
                        long unit = pow10(6 - precision);
                        type.writeLong(builder, micros / unit * unit);
                    }
                    else {
                        long unit = pow10(12 - precision);
                        type.writeObject(builder, new LongInterval(micros, (int) (987_654 / unit * unit)));
                    }
                }
                builder.appendNull();
            });
        }
        assertRoundTrip(INTERVAL_DAY_TIME, builder -> {
            INTERVAL_DAY_TIME.writeLong(builder, 1_234_567L);
            INTERVAL_DAY_TIME.writeLong(builder, Long.MAX_VALUE);
            INTERVAL_DAY_TIME.writeLong(builder, Long.MIN_VALUE);
        });
    }

    @Test
    public void testLegacyIntervalWithoutParametricInterval()
            throws IOException
    {
        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, 9, 6);
        assertRoundTrip(sessionWithoutCapability(ClientCapabilities.PARAMETRIC_INTERVAL), ImmutableList.of(typed("col", type)), page(block(type, builder -> {
            type.writeLong(builder, 1_234_567L);
            type.writeLong(builder, -1_234_567L);
            type.writeLong(builder, 1_234_500L);
            type.writeLong(builder, 0);
            builder.appendNull();
        })));
    }

    @Test
    public void testLegacyDateTimeWithoutParametricDateTime()
            throws IOException
    {
        Session session = sessionWithoutCapability(ClientCapabilities.PARAMETRIC_DATETIME);
        TimestampType timestamp = createTimestampType(9);
        TimestampWithTimeZoneType timestampWithTimeZone = createTimestampWithTimeZoneType(9);
        Type time = createTimeType(9);
        TimeWithTimeZoneType timeWithTimeZone = createTimeWithTimeZoneType(12);
        assertRoundTrip(
                session,
                ImmutableList.of(typed("a", timestamp), typed("b", timestampWithTimeZone), typed("c", time), typed("d", timeWithTimeZone), typed("e", createTimestampType(0))),
                page(
                        block(timestamp, builder -> {
                            timestamp.writeObject(builder, new LongTimestamp(1_600_000_000_123_456L, 789_000));
                            timestamp.writeObject(builder, new LongTimestamp(1_600_000_000_999_999L, 999_000));
                            builder.appendNull();
                        }),
                        block(timestampWithTimeZone, builder -> {
                            timestampWithTimeZone.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_600_000_000_123L, 456_789_000, getTimeZoneKey("Europe/Warsaw")));
                            timestampWithTimeZone.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_600_000_000_999L, 999_999_000, UTC_KEY));
                            builder.appendNull();
                        }),
                        block(time, builder -> {
                            time.writeLong(builder, 45_296_123_456_789_000L);
                            time.writeLong(builder, 86_399_999_999_999_000L);
                            builder.appendNull();
                        }),
                        block(timeWithTimeZone, builder -> {
                            timeWithTimeZone.writeObject(builder, new LongTimeWithTimeZone(45_296_123_456_789_000L, 120));
                            timeWithTimeZone.writeObject(builder, new LongTimeWithTimeZone(86_399_999_999_999_000L, -120));
                            builder.appendNull();
                        }),
                        block(createTimestampType(0), builder -> {
                            createTimestampType(0).writeLong(builder, 1_600_000_000_000_000L);
                            builder.appendNull();
                            builder.appendNull();
                        })));
    }

    @Test
    public void testUuid()
            throws IOException
    {
        assertRoundTrip(UUID, builder -> {
            UUID.writeSlice(builder, javaUuidToTrinoUuid(java.util.UUID.fromString("12345678-1234-5678-1234-567812345678")));
            UUID.writeSlice(builder, javaUuidToTrinoUuid(new UUID(0, 0)));
            UUID.writeSlice(builder, javaUuidToTrinoUuid(new UUID(-1, -1)));
            UUID.writeSlice(builder, javaUuidToTrinoUuid(new UUID(Long.MIN_VALUE, 1)));
            builder.appendNull();
        });
    }

    @Test
    public void testIpAddress()
            throws IOException
    {
        assertRoundTrip(IPADDRESS, builder -> {
            IPADDRESS.writeSlice(builder, ipAddress("1.2.3.4"));
            IPADDRESS.writeSlice(builder, ipAddress("255.255.255.255"));
            IPADDRESS.writeSlice(builder, ipAddress("0.0.0.0"));
            IPADDRESS.writeSlice(builder, ipAddress("2001:db8::1"));
            IPADDRESS.writeSlice(builder, ipAddress("::"));
            IPADDRESS.writeSlice(builder, ipAddress("fe80::1ff:fe23:4567:890a"));
            IPADDRESS.writeSlice(builder, ipAddress("2001:db8::1:0:0:1"));
            builder.appendNull();
        });
    }

    @Test
    public void testNumber()
            throws IOException
    {
        Consumer<BlockBuilder> values = builder -> {
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("1.5")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("-1.5")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("0")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("123456789012345678901234567890.123456789")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("1E+10")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("1E-10")));
            NUMBER.writeObject(builder, TrinoNumber.from(new BigDecimal("0.000001")));
            NUMBER.writeObject(builder, TrinoNumber.from(new TrinoNumber.NotANumber()));
            NUMBER.writeObject(builder, TrinoNumber.from(new TrinoNumber.Infinity(false)));
            NUMBER.writeObject(builder, TrinoNumber.from(new TrinoNumber.Infinity(true)));
            builder.appendNull();
        };
        assertRoundTrip(NUMBER, values);
        // A client without the NUMBER capability sees the column as varchar
        assertRoundTrip(sessionWithoutCapability(ClientCapabilities.NUMBER), ImmutableList.of(typed("col", NUMBER)), page(block(NUMBER, values)));
    }

    @Test
    public void testVariant()
            throws IOException
    {
        Consumer<BlockBuilder> values = builder -> {
            VARIANT.writeObject(builder, Variant.ofObject(Map.of(
                    utf8Slice("a"), Variant.ofInt(1),
                    utf8Slice("b"), Variant.ofArray(List.of(Variant.ofBoolean(true), Variant.NULL_VALUE)))));
            VARIANT.writeObject(builder, Variant.ofLong(42));
            VARIANT.writeObject(builder, Variant.NULL_VALUE);
            builder.appendNull();
        };
        assertRoundTrip(VARIANT, values);
        assertRoundTrip(sessionWithoutCapability(ClientCapabilities.VARIANT_BINARY), ImmutableList.of(typed("col", VARIANT)), page(block(VARIANT, values)));
        assertRoundTrip(sessionWithoutVariantCapabilities(), ImmutableList.of(typed("col", VARIANT)), page(block(VARIANT, values)));

        ArrayType arrayOfVariant = new ArrayType(VARIANT);
        Consumer<BlockBuilder> arrays = builder -> ((ArrayBlockBuilder) builder).buildEntry(values::accept);
        assertRoundTrip(arrayOfVariant, arrays);
        assertRoundTrip(sessionWithoutCapability(ClientCapabilities.VARIANT_BINARY), ImmutableList.of(typed("col", arrayOfVariant)), page(block(arrayOfVariant, arrays)));
    }

    @Test
    public void testArray()
            throws IOException
    {
        ArrayType arrayOfBigint = new ArrayType(BIGINT);
        assertRoundTrip(arrayOfBigint, builder -> {
            ((ArrayBlockBuilder) builder).buildEntry(elements -> {
                for (int i = 0; i < 10; i++) {
                    BIGINT.writeLong(elements, i);
                }
                elements.appendNull();
            });
            ((ArrayBlockBuilder) builder).buildEntry(_ -> {});
            builder.appendNull();
        });

        ArrayType arrayOfArrayOfVarchar = new ArrayType(new ArrayType(VARCHAR));
        assertRoundTrip(arrayOfArrayOfVarchar, builder -> {
            ((ArrayBlockBuilder) builder).buildEntry(outer -> {
                ((ArrayBlockBuilder) outer).buildEntry(inner -> {
                    VARCHAR.writeSlice(inner, utf8Slice("a"));
                    inner.appendNull();
                    VARCHAR.writeSlice(inner, utf8Slice("b"));
                });
                outer.appendNull();
                ((ArrayBlockBuilder) outer).buildEntry(_ -> {});
            });
            builder.appendNull();
        });

        TimestampType timestamp = createTimestampType(9);
        ArrayType arrayOfTimestamp = new ArrayType(timestamp);
        assertRoundTrip(arrayOfTimestamp, builder -> ((ArrayBlockBuilder) builder).buildEntry(elements -> {
            timestamp.writeObject(elements, new LongTimestamp(1_600_000_000_123_456L, 789_000));
            elements.appendNull();
        }));
    }

    @Test
    public void testMap()
            throws IOException
    {
        MapType varcharToBigint = mapType(VARCHAR, BIGINT);
        assertRoundTrip(varcharToBigint, builder -> {
            ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
                VARCHAR.writeSlice(keys, utf8Slice("a"));
                BIGINT.writeLong(values, 1);
                VARCHAR.writeSlice(keys, utf8Slice("b"));
                values.appendNull();
                VARCHAR.writeSlice(keys, utf8Slice(""));
                BIGINT.writeLong(values, -1);
            });
            ((MapBlockBuilder) builder).buildEntry((_, _) -> {});
            builder.appendNull();
        });

        for (Type keyType : ImmutableList.of(BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, BOOLEAN, DATE, createDecimalType(5, 2), createTimestampType(3), UUID, createCharType(3))) {
            MapType mapType = mapType(keyType, DOUBLE);
            assertRoundTrip(mapType, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
                writeSampleKey(keyType, keys, 0);
                DOUBLE.writeDouble(values, 0.5);
                writeSampleKey(keyType, keys, 1);
                values.appendNull();
            }));
        }

        // varbinary keys decode to byte arrays, which compare by identity, so compare the entries by their content
        MapType varbinaryToDouble = mapType(VARBINARY, DOUBLE);
        Page page = page(block(varbinaryToDouble, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
            writeSampleKey(VARBINARY, keys, 0);
            DOUBLE.writeDouble(values, 0.5);
            writeSampleKey(VARBINARY, keys, 1);
            values.appendNull();
        })));
        List<TypedColumn> columns = ImmutableList.of(typed("col", varbinaryToDouble));
        for (String encoding : PROTOBUF_ENCODINGS) {
            List<List<Object>> decoded = decode(TEST_SESSION, columns, encoding, ImmutableList.of(page));
            assertThat(decoded).hasSize(1);
            Map<?, ?> map = (Map<?, ?>) decoded.get(0).get(0);
            Map<String, Object> byContent = new HashMap<>();
            map.forEach((key, value) -> byContent.put(new String((byte[]) key, UTF_8), value));
            Map<String, Object> expected = new HashMap<>();
            expected.put("key0", 0.5);
            expected.put("key1", null);
            assertThat(byContent).isEqualTo(expected);
        }

        MapType bigintToArray = mapType(BIGINT, new ArrayType(DOUBLE));
        assertRoundTrip(bigintToArray, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
            BIGINT.writeLong(keys, 1);
            ((ArrayBlockBuilder) values).buildEntry(elements -> {
                DOUBLE.writeDouble(elements, 1.0);
                elements.appendNull();
            });
            BIGINT.writeLong(keys, 2);
            values.appendNull();
        }));
    }

    @Test
    public void testRow()
            throws IOException
    {
        RowType inner = RowType.rowType(RowType.field("d", DOUBLE), RowType.field(new ArrayType(VARCHAR)));
        RowType rowType = RowType.rowType(RowType.field("a", BIGINT), RowType.field("b", VARCHAR), RowType.field("c", BOOLEAN), RowType.field("nested", inner));
        assertRoundTrip(rowType, builder -> {
            ((RowBlockBuilder) builder).buildEntry(fields -> {
                BIGINT.writeLong(fields.get(0), 1);
                VARCHAR.writeSlice(fields.get(1), utf8Slice("ala"));
                BOOLEAN.writeBoolean(fields.get(2), true);
                ((RowBlockBuilder) fields.get(3)).buildEntry(innerFields -> {
                    DOUBLE.writeDouble(innerFields.get(0), 0.5);
                    ((ArrayBlockBuilder) innerFields.get(1)).buildEntry(elements -> VARCHAR.writeSlice(elements, utf8Slice("x")));
                });
            });
            ((RowBlockBuilder) builder).buildEntry(fields -> {
                fields.get(0).appendNull();
                fields.get(1).appendNull();
                fields.get(2).appendNull();
                fields.get(3).appendNull();
            });
            builder.appendNull();
        });

        ArrayType arrayOfRow = new ArrayType(rowType);
        assertRoundTrip(arrayOfRow, builder -> ((ArrayBlockBuilder) builder).buildEntry(elements -> {
            ((RowBlockBuilder) elements).buildEntry(fields -> {
                BIGINT.writeLong(fields.get(0), 7);
                fields.get(1).appendNull();
                BOOLEAN.writeBoolean(fields.get(2), false);
                fields.get(3).appendNull();
            });
            elements.appendNull();
        }));
    }

    @Test
    public void testTypesWithoutDedicatedEncoding()
            throws IOException
    {
        // rendered as text
        assertRoundTrip(COLOR, builder -> {
            COLOR.writeLong(builder, 0x112233);
            COLOR.writeLong(builder, -1);
            builder.appendNull();
        });
        // rendered as bytes
        assertRoundTrip(HYPER_LOG_LOG, builder -> {
            HYPER_LOG_LOG.writeSlice(builder, wrappedBuffer(new byte[] {1, 2, 3}));
            builder.appendNull();
        });
        // rendered as a JSON document, which the json decoder of the client cannot read for a type it does not know,
        // so the expected value is spelled out
        CustomType customType = new CustomType();
        List<TypedColumn> columns = ImmutableList.of(typed("col", customType));
        Page page = page(block(customType, builder -> {
            customType.writeInt(builder, 42);
            builder.appendNull();
        }));
        for (String encoding : PROTOBUF_ENCODINGS) {
            assertThat(decode(TEST_SESSION, columns, encoding, ImmutableList.of(page)))
                    .isEqualTo(ImmutableList.of(ImmutableList.of(Map.of("value", 42)), singletonList(null)));
        }
        // only nulls
        assertRoundTrip(UNKNOWN, builder -> {
            builder.appendNull();
            builder.appendNull();
        });
    }

    @Test
    public void testMultiplePagesAndColumns()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("a", BIGINT), typed("b", VARCHAR), typed("c", DATE));
        Page first = page(
                block(BIGINT, builder -> {
                    BIGINT.writeLong(builder, 1);
                    BIGINT.writeLong(builder, 2);
                }),
                block(VARCHAR, builder -> {
                    VARCHAR.writeSlice(builder, utf8Slice("one"));
                    builder.appendNull();
                }),
                block(DATE, builder -> {
                    DATE.writeLong(builder, 1);
                    DATE.writeLong(builder, 2);
                }));
        Page second = page(
                block(BIGINT, builder -> BIGINT.writeLong(builder, 3)),
                block(VARCHAR, builder -> VARCHAR.writeSlice(builder, utf8Slice("three"))),
                block(DATE, builder -> builder.appendNull()));
        Page empty = page(
                block(BIGINT, _ -> {}),
                block(VARCHAR, _ -> {}),
                block(DATE, _ -> {}));
        assertRoundTrip(TEST_SESSION, columns, first, empty, second);
        assertRoundTrip(TEST_SESSION, columns);
    }

    @Test
    public void testCompressedSegments()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("a", BIGINT), typed("b", VARCHAR), typed("c", createDecimalType(12, 2)));
        Page page = page(
                block(BIGINT, builder -> {
                    for (int i = 0; i < 10_000; i++) {
                        BIGINT.writeLong(builder, i * 7919L);
                    }
                }),
                block(VARCHAR, builder -> {
                    for (int i = 0; i < 10_000; i++) {
                        if (i % 13 == 0) {
                            builder.appendNull();
                        }
                        else {
                            VARCHAR.writeSlice(builder, utf8Slice("value " + i));
                        }
                    }
                }),
                block(createDecimalType(12, 2), builder -> {
                    for (int i = 0; i < 10_000; i++) {
                        createDecimalType(12, 2).writeLong(builder, i * 101L);
                    }
                }));

        List<List<Object>> expected = decode(TEST_SESSION, columns, "json", ImmutableList.of(page));
        for (String encoding : PROTOBUF_ENCODINGS) {
            assertThat(decode(TEST_SESSION, columns, encoding, ImmutableList.of(page)))
                    .usingRecursiveComparison()
                    .isEqualTo(expected);
        }
        // The compressed segment is smaller than the uncompressed one
        byte[] uncompressed = encode(TEST_SESSION, columns, "protobuf", ImmutableList.of(page)).data();
        byte[] compressed = encode(TEST_SESSION, columns, "protobuf+zstd", ImmutableList.of(page)).data();
        assertThat(compressed.length).isLessThan(uncompressed.length);
    }

    @Test
    public void testSegmentSizeAttribute()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("a", BIGINT));
        Page page = page(block(BIGINT, builder -> {
            BIGINT.writeLong(builder, 1);
            BIGINT.writeLong(builder, 2);
        }));
        Encoded encoded = encode(TEST_SESSION, columns, "protobuf", ImmutableList.of(page));
        assertThat(encoded.attributes().get(DataAttribute.SEGMENT_SIZE, Integer.class)).isEqualTo(encoded.data().length);
        // Segment { Row { Value { bigint: 1 } }, Row { Value { bigint: 2 } } }
        assertThat(encoded.data()).containsExactly(0x0A, 0x04, 0x0A, 0x02, 0x30, 0x02, 0x0A, 0x04, 0x0A, 0x02, 0x30, 0x04);
    }

    @Test
    public void testNullEncoding()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("a", VARCHAR));
        Page page = page(block(VARCHAR, BlockBuilder::appendNull));
        Encoded encoded = encode(TEST_SESSION, columns, "protobuf", ImmutableList.of(page));
        // Segment { Row { Value { null: NULL_VALUE } } }
        assertThat(encoded.data()).containsExactly(0x0A, 0x04, 0x0A, 0x02, 0x08, 0x00);
    }

    @Test
    public void testMismatchedColumnTypeIsRejected()
            throws IOException
    {
        Page page = page(block(BIGINT, builder -> BIGINT.writeLong(builder, 1)));
        Encoded encoded = encode(TEST_SESSION, ImmutableList.of(typed("a", BIGINT)), "protobuf", ImmutableList.of(page));
        assertThatThrownBy(() -> decode(TEST_SESSION, ImmutableList.of(typed("a", VARCHAR)), "protobuf", encoded))
                .hasMessageContaining("Expected value field 10 but got 6");
        assertThatThrownBy(() -> decode(TEST_SESSION, ImmutableList.of(typed("a", BIGINT), typed("b", BIGINT)), "protobuf", encoded))
                .hasMessageContaining("Row has 1 values but 2 columns were expected");
    }

    @Test
    public void testConcurrentEncoding()
            throws Exception
    {
        // One encoder writes the segments of every driver of a query (see OutputSpoolingOperatorFactory),
        // so segments encoded at the same time must not share state
        TimestampWithTimeZoneType timestampWithTimeZone = createTimestampWithTimeZoneType(3);
        List<TypedColumn> columns = ImmutableList.of(typed("a", BIGINT), typed("b", VARCHAR), typed("c", timestampWithTimeZone), typed("d", new ArrayType(createDecimalType(30, 2))));
        List<TimeZoneKey> zones = ImmutableList.of(UTC_KEY, getTimeZoneKey("Europe/Warsaw"), getTimeZoneKey("America/Los_Angeles"), getTimeZoneKey("Asia/Kathmandu"));
        DecimalType decimal = createDecimalType(30, 2);
        int rows = 2_000;
        List<Page> pages = new ArrayList<>();
        for (int segment = 0; segment < 8; segment++) {
            int seed = segment;
            pages.add(page(
                    block(BIGINT, builder -> {
                        for (int i = 0; i < rows; i++) {
                            BIGINT.writeLong(builder, seed * 1_000_000L + i);
                        }
                    }),
                    block(VARCHAR, builder -> {
                        for (int i = 0; i < rows; i++) {
                            VARCHAR.writeSlice(builder, utf8Slice("segment " + seed + " row " + i + "x".repeat(i % 50)));
                        }
                    }),
                    block(timestampWithTimeZone, builder -> {
                        for (int i = 0; i < rows; i++) {
                            timestampWithTimeZone.writeLong(builder, packDateTimeWithZone(i * 1_000_003L, zones.get((i + seed) % zones.size())));
                        }
                    }),
                    block(new ArrayType(decimal), builder -> {
                        for (int i = 0; i < rows; i++) {
                            int value = i;
                            ((ArrayBlockBuilder) builder).buildEntry(elements -> {
                                for (int j = 0; j < value % 5; j++) {
                                    decimal.writeObject(elements, Int128.valueOf(seed * 1_000_000_000_000_000_000L + value * (long) j));
                                }
                            });
                        }
                    })));
        }
        List<List<List<Object>>> expected = new ArrayList<>();
        for (Page page : pages) {
            expected.add(decode(TEST_SESSION, columns, "json", ImmutableList.of(page)));
        }

        ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            outputColumns.add(new OutputColumn(i, columns.get(i).name(), columns.get(i).type()));
        }
        ExecutorService executor = newFixedThreadPool(pages.size());
        try {
            for (String encoding : PROTOBUF_ENCODINGS) {
                QueryDataEncoder encoder = createEncoder(encoding).create(TEST_SESSION, outputColumns.build());
                for (int round = 0; round < 5; round++) {
                    CyclicBarrier barrier = new CyclicBarrier(pages.size());
                    List<Future<Encoded>> futures = new ArrayList<>();
                    for (Page page : pages) {
                        futures.add(executor.submit(() -> {
                            barrier.await();
                            ByteArrayOutputStream output = new ByteArrayOutputStream();
                            DataAttributes attributes = encoder.encodeTo(output, ImmutableList.of(page));
                            return new Encoded(output.toByteArray(), attributes);
                        }));
                    }
                    for (int i = 0; i < pages.size(); i++) {
                        assertThat(decode(TEST_SESSION, columns, encoding, futures.get(i).get()))
                                .as("encoding %s, segment %s", encoding, i)
                                .isEqualTo(expected.get(i));
                    }
                }
                encoder.close();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTimestampWithTimeZoneOffsets()
            throws IOException
    {
        // Historical local mean time offsets have seconds, and daylight saving transitions have gaps and overlaps;
        // the client renders the offset the server resolved, so the local date-time matches the json encoding's
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(3);
        List<TimeZoneKey> zones = ImmutableList.of(getTimeZoneKey("Europe/Warsaw"), getTimeZoneKey("America/New_York"), getTimeZoneKey("Asia/Kathmandu"), getTimeZoneKey("Australia/Lord_Howe"), getTimeZoneKey("-14:00"), getTimeZoneKey("+14:00"));
        long[] instants = {
                -2_840_145_840_000L, // 1880, local mean time
                -1_640_995_200_000L, // 1918
                1_615_705_199_999L, // 2021-03-14 06:59:59.999 UTC, before the New York gap
                1_615_705_200_000L,
                1_636_264_799_999L, // 2021-11-07 05:59:59.999 UTC, before the New York overlap
                1_636_264_800_000L,
        };
        assertRoundTrip(type, builder -> {
            for (TimeZoneKey zone : zones) {
                for (long instant : instants) {
                    type.writeLong(builder, packDateTimeWithZone(instant, zone));
                }
            }
        });
    }

    @Test
    public void testLegacyIntervalMapKeysCollide()
            throws IOException
    {
        // Without PARAMETRIC_INTERVAL, keys are rounded to milliseconds, which can make two keys equal.
        // The json encoding fails instead of dropping an entry, and so does the protobuf encoding.
        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, 9, 6);
        MapType mapType = mapType(type, BIGINT);
        Page colliding = page(block(mapType, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
            type.writeLong(keys, 1_000_100L);
            BIGINT.writeLong(values, 1);
            type.writeLong(keys, 1_000_200L);
            BIGINT.writeLong(values, 2);
        })));
        Session session = sessionWithoutCapability(ClientCapabilities.PARAMETRIC_INTERVAL);
        List<TypedColumn> columns = ImmutableList.of(typed("col", mapType));
        for (String encoding : ImmutableList.<String>builder().add("json").addAll(PROTOBUF_ENCODINGS).build()) {
            assertThatThrownBy(() -> encode(session, columns, encoding, ImmutableList.of(colliding)))
                    .as("encoding %s", encoding)
                    .hasMessage("Interval map keys collide at millisecond precision; use a client supporting PARAMETRIC_INTERVAL");
        }

        // a collision of keys nested in a row key
        RowType rowKey = RowType.anonymousRow(type);
        MapType nested = mapType(rowKey, BIGINT);
        Page nestedColliding = page(block(nested, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
            ((RowBlockBuilder) keys).buildEntry(fields -> type.writeLong(fields.getFirst(), 1_000_100L));
            BIGINT.writeLong(values, 1);
            ((RowBlockBuilder) keys).buildEntry(fields -> type.writeLong(fields.getFirst(), 1_000_200L));
            BIGINT.writeLong(values, 2);
        })));
        for (String encoding : ImmutableList.<String>builder().add("json").addAll(PROTOBUF_ENCODINGS).build()) {
            assertThatThrownBy(() -> encode(session, ImmutableList.of(typed("col", nested)), encoding, ImmutableList.of(nestedColliding)))
                    .as("encoding %s", encoding)
                    .hasMessage("Interval map keys collide at millisecond precision; use a client supporting PARAMETRIC_INTERVAL");
        }

        // keys that stay distinct after rounding, and the colliding keys for a client with the capability
        assertRoundTrip(session, columns, page(block(mapType, builder -> ((MapBlockBuilder) builder).buildEntry((keys, values) -> {
            type.writeLong(keys, 1_000_100L);
            BIGINT.writeLong(values, 1);
            type.writeLong(keys, 1_002_000L);
            BIGINT.writeLong(values, 2);
        }))));
        assertRoundTrip(TEST_SESSION, columns, colliding);
    }

    @Test
    public void testLegacyIntervalOutOfRange()
            throws IOException
    {
        // The json encoding formats the rounded legacy value as text, so it can exceed the microsecond range;
        // the protobuf encoding sends microseconds and fails for the two values within half a millisecond of the
        // ends of the range. Only clients without PARAMETRIC_INTERVAL are affected.
        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, 9, 6);
        Session session = sessionWithoutCapability(ClientCapabilities.PARAMETRIC_INTERVAL);
        List<TypedColumn> columns = ImmutableList.of(typed("col", type));
        Page page = page(block(type, builder -> type.writeLong(builder, Long.MAX_VALUE)));
        assertThat(decode(session, columns, "json", ImmutableList.of(page)))
                .isEqualTo(ImmutableList.of(ImmutableList.of("106751991 04:00:54.776")));
        for (String encoding : PROTOBUF_ENCODINGS) {
            assertThatThrownBy(() -> encode(session, columns, encoding, ImmutableList.of(page)))
                    .hasMessage("Value out of range for an interval after rounding");
        }
        assertRoundTrip(session, columns, page(block(type, builder -> {
            type.writeLong(builder, Long.MAX_VALUE - 500);
            type.writeLong(builder, Long.MIN_VALUE + 500);
        })));
    }

    @Test
    public void testVariantJsonTextIsSentAsWritten()
            throws IOException
    {
        // Without VARIANT_BINARY the client gets the variant's JSON text. The json decoder of the client copies the
        // JSON value through a parser, which normalizes numbers (1.50 becomes 1.5, and a decimal beyond double
        // precision is rounded); the protobuf decoder returns the text the server wrote, without that loss.
        List<Variant> variants = ImmutableList.of(
                Variant.ofDecimal(new BigDecimal("1.50")),
                Variant.ofDecimal(new BigDecimal("12345678901234567890.123")),
                Variant.ofDouble(0.1),
                Variant.ofDouble(1e300),
                Variant.ofFloat(1.1f),
                Variant.ofString("quote \" line separator \u2028 control \u0001"),
                Variant.ofArray(List.of(Variant.ofDouble(2.5), Variant.ofLong(Long.MAX_VALUE), Variant.NULL_VALUE)));
        Session session = sessionWithoutCapability(ClientCapabilities.VARIANT_BINARY);
        List<TypedColumn> columns = ImmutableList.of(typed("col", VARIANT));
        Page page = page(block(VARIANT, builder -> variants.forEach(variant -> VARIANT.writeObject(builder, variant))));
        List<List<Object>> expected = variants.stream()
                .map(variant -> ImmutableList.<Object>of(VariantUtil.asJson(variant).toStringUtf8()))
                .collect(toImmutableList());
        assertThat(expected.get(0)).isEqualTo(ImmutableList.of("1.50"));
        for (String encoding : PROTOBUF_ENCODINGS) {
            assertThat(decode(session, columns, encoding, ImmutableList.of(page)))
                    .as("encoding %s", encoding)
                    .isEqualTo(expected);
        }
        assertThat(decode(session, columns, "json", ImmutableList.of(page)).get(0)).isEqualTo(ImmutableList.of("1.5"));
    }

    @Test
    public void testMalformedUtf8Varchar()
            throws IOException
    {
        // A varchar is not guaranteed to be valid UTF-8. The json decoder of the client fails on it; the protobuf
        // decoder replaces each malformed sequence with U+FFFD, like new String(bytes, UTF_8)
        List<TypedColumn> columns = ImmutableList.of(typed("col", VARCHAR));
        Page page = page(block(VARCHAR, builder -> VARCHAR.writeSlice(builder, wrappedBuffer(new byte[] {'a', (byte) 0xC3, 'b', (byte) 0xFF}))));
        assertThatThrownBy(() -> decode(TEST_SESSION, columns, "json", ImmutableList.of(page)))
                .hasMessageContaining("Invalid UTF-8");
        for (String encoding : PROTOBUF_ENCODINGS) {
            assertThat(decode(TEST_SESSION, columns, encoding, ImmutableList.of(page)))
                    .isEqualTo(ImmutableList.of(ImmutableList.of("a\uFFFDb\uFFFD")));
        }
    }

    private void assertRoundTrip(Type type, Consumer<BlockBuilder> values)
            throws IOException
    {
        assertRoundTrip(TEST_SESSION, ImmutableList.of(typed("col", type)), page(block(type, values)));
    }

    private void assertRoundTrip(Session session, List<TypedColumn> columns, Page... pages)
            throws IOException
    {
        List<List<Object>> expected = decode(session, columns, "json", ImmutableList.copyOf(pages));
        int rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        assertThat(expected).hasSize(rows);
        for (String encoding : PROTOBUF_ENCODINGS) {
            List<List<Object>> actual = decode(session, columns, encoding, ImmutableList.copyOf(pages));
            assertThat(actual)
                    .as("encoding %s", encoding)
                    .usingRecursiveComparison()
                    // NaN is equal to itself here
                    .withEqualsForType((left, right) -> Float.compare(left, right) == 0, Float.class)
                    .withEqualsForType((left, right) -> Double.compare(left, right) == 0, Double.class)
                    .isEqualTo(expected);
        }
    }

    private record Encoded(byte[] data, DataAttributes attributes) {}

    private static Encoded encode(Session session, List<TypedColumn> columns, String encoding, List<Page> pages)
            throws IOException
    {
        ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            outputColumns.add(new OutputColumn(i, columns.get(i).name(), columns.get(i).type()));
        }
        QueryDataEncoder encoder = createEncoder(encoding).create(session, outputColumns.build());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataAttributes attributes = encoder.encodeTo(output, pages);
        encoder.close();
        return new Encoded(output.toByteArray(), attributes);
    }

    private static List<List<Object>> decode(Session session, List<TypedColumn> columns, String encoding, List<Page> pages)
            throws IOException
    {
        return decode(session, columns, encoding, encode(session, columns, encoding, pages));
    }

    private static List<List<Object>> decode(Session session, List<TypedColumn> columns, String encoding, Encoded encoded)
            throws IOException
    {
        boolean supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());
        boolean supportsParametricInterval = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_INTERVAL.toString());
        boolean supportsNumber = session.getClientCapabilities().contains(ClientCapabilities.NUMBER.toString());
        boolean supportsVariant = session.getClientCapabilities().contains(ClientCapabilities.VARIANT.toString());
        boolean supportsVariantBinary = session.getClientCapabilities().contains(ClientCapabilities.VARIANT_BINARY.toString());
        ImmutableList.Builder<Column> clientColumns = ImmutableList.builderWithExpectedSize(columns.size());
        for (TypedColumn column : columns) {
            clientColumns.add(createColumn(column.name(), column.type(), supportsParametricDateTime, supportsNumber, supportsVariant, supportsVariantBinary, supportsParametricInterval));
        }
        QueryDataDecoder decoder = QueryDataDecoders.get(encoding).create(clientColumns.build(), DataAttributes.empty(), supportsVariantBinary);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(encoded.data()), encoded.attributes())) {
            return ImmutableList.copyOf(iterator);
        }
    }

    private static QueryDataEncoder.Factory createEncoder(String encoding)
    {
        QueryDataEncodingConfigStub config = new QueryDataEncodingConfigStub();
        return switch (encoding) {
            case "json" -> new JsonQueryDataEncoder.Factory(new JsonMapper());
            case "protobuf" -> new ProtobufQueryDataEncoder.Factory(new JsonMapper());
            case "protobuf+zstd" -> new ProtobufQueryDataEncoder.ZstdFactory(config, new JsonMapper());
            case "protobuf+lz4" -> new ProtobufQueryDataEncoder.Lz4Factory(config, new JsonMapper());
            default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
        };
    }

    private static class QueryDataEncodingConfigStub
            extends QueryDataEncodingConfig
    {
        public QueryDataEncodingConfigStub()
        {
            setCompressionThreshold(DataSize.ofBytes(COMPRESSION_THRESHOLD));
        }
    }

    private static void writeSampleKey(Type type, BlockBuilder builder, int index)
    {
        switch (type) {
            case Type t when t.equals(BOOLEAN) -> BOOLEAN.writeBoolean(builder, index == 0);
            case Type t when t.equals(DOUBLE) -> DOUBLE.writeDouble(builder, index + 0.5);
            case Type t when t.equals(REAL) -> REAL.writeFloat(builder, index + 0.5f);
            case Type t when t.equals(UUID) -> UUID.writeSlice(builder, javaUuidToTrinoUuid(new UUID(index, index)));
            case Type t when t.equals(VARBINARY) -> VARBINARY.writeSlice(builder, utf8Slice("key" + index));
            case Type t when t.getJavaType() == Slice.class -> t.writeSlice(builder, utf8Slice("k" + index));
            case TimestampType t -> t.writeLong(builder, (index + 1) * 1000L);
            case Type t when t.getJavaType() == long.class -> t.writeLong(builder, index + 1);
            default -> throw new IllegalArgumentException("Unsupported key type: " + type);
        }
    }

    private static Slice ipAddress(String address)
    {
        byte[] bytes = InetAddress.ofLiteral(address).getAddress();
        if (bytes.length == 4) {
            byte[] mapped = new byte[16];
            mapped[10] = (byte) 0xFF;
            mapped[11] = (byte) 0xFF;
            System.arraycopy(bytes, 0, mapped, 12, 4);
            bytes = mapped;
        }
        try {
            // sanity check: the mapped address renders back to the original
            assertThat(InetAddresses.toAddrString(InetAddress.getByAddress(bytes))).isEqualTo(address);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        return wrappedBuffer(bytes);
    }

    private static long pow10(int exponent)
    {
        long result = 1;
        for (int i = 0; i < exponent; i++) {
            result *= 10;
        }
        return result;
    }

    private static Block block(Type type, Consumer<BlockBuilder> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 10);
        values.accept(builder);
        return builder.build();
    }

    private static Page page(Block... blocks)
    {
        return new Page(blocks);
    }

    private static Session sessionWithoutCapability(ClientCapabilities capability)
    {
        return Session.builder(TEST_SESSION)
                .setClientCapabilities(TEST_SESSION.getClientCapabilities().stream()
                        .filter(value -> !value.equals(capability.toString()))
                        .collect(toImmutableSet()))
                .build();
    }

    private static Session sessionWithoutVariantCapabilities()
    {
        return Session.builder(TEST_SESSION)
                .setClientCapabilities(TEST_SESSION.getClientCapabilities().stream()
                        .filter(value -> !value.equals(ClientCapabilities.VARIANT.toString()))
                        .filter(value -> !value.equals(ClientCapabilities.VARIANT_BINARY.toString()))
                        .collect(toImmutableSet()))
                .build();
    }

    record TypedColumn(String name, Type type)
    {
        TypedColumn
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }
    }

    private static TypedColumn typed(String name, Type type)
    {
        return new TypedColumn(name, type);
    }

    private static final class CustomType
            extends AbstractIntType
    {
        private CustomType()
        {
            super(new TypeDescriptor("custom"));
        }

        @Override
        public String getDisplayName()
        {
            return "custom";
        }

        @Override
        public Object getObjectValue(Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }
            return new Custom(getInt(block, position));
        }
    }

    private record Custom(@JsonProperty("value") int value) {}
}
