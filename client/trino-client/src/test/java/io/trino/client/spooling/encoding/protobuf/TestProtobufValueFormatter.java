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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatDate;
import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatDecimal;
import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatTime;
import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatTimeWithTimeZone;
import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatTimestamp;
import static io.trino.client.spooling.encoding.protobuf.ProtobufValueFormatter.formatTimestampWithTimeZone;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the hand-written formats to the java.time and BigDecimal formats the server uses for the json
 * encoding, over the edges of each range and random values.
 */
public class TestProtobufValueFormatter
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    private static final long PICOSECONDS_PER_DAY = 24 * 60 * 60 * PICOSECONDS_PER_SECOND;
    private static final int[] YEARS = {-999_999_999, -292_275, -10000, -9999, -1000, -999, -100, -99, -10, -9, -1, 0, 1, 9, 10, 99, 100, 999, 1000, 1582, 1600, 1900, 1970, 2000, 2024, 9999, 10000, 99999, 294247, 999_999_999};

    @Test
    public void testDate()
    {
        List<Long> days = new ArrayList<>();
        for (int year : YEARS) {
            for (int month = 1; month <= 12; month++) {
                LocalDate first = LocalDate.of(year, month, 1);
                days.add(first.toEpochDay());
                days.add(first.toEpochDay() + first.lengthOfMonth() - 1);
            }
        }
        for (long day = -1000; day <= 1000; day++) {
            days.add(day);
        }
        days.add((long) Integer.MIN_VALUE);
        days.add((long) Integer.MAX_VALUE);
        Random random = new Random(1);
        for (int i = 0; i < 100_000; i++) {
            days.add((long) random.nextInt());
        }
        for (long day : days) {
            assertThat(formatDate(day)).as("day %s", day).isEqualTo(LocalDate.ofEpochDay(day).toString());
        }
    }

    @Test
    public void testTimestamp()
    {
        List<Long> values = new ArrayList<>();
        for (int year : YEARS) {
            if (year < -292_275 || year > 292_277) {
                // outside the range of epoch microseconds
                continue;
            }
            values.add(LocalDateTime.of(year, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000);
            values.add(LocalDateTime.of(year, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 999_999);
            values.add(LocalDateTime.of(year, 2, 28, 12, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 1);
        }
        for (long micros = -2_000_000; micros <= 2_000_000; micros += 7) {
            values.add(micros);
        }
        values.add(Long.MIN_VALUE);
        values.add(Long.MAX_VALUE);
        Random random = new Random(2);
        for (int i = 0; i < 20_000; i++) {
            values.add(random.nextLong());
        }
        for (long epochMicros : values) {
            int picosOfMicro = random.nextInt(1_000_000);
            for (int precision = 0; precision <= 12; precision++) {
                String expected = expectedTimestamp(precision, epochMicros, picosOfMicro, 0);
                assertThat(formatTimestamp(precision, epochMicros, picosOfMicro))
                        .as("micros %s picos %s precision %s", epochMicros, picosOfMicro, precision)
                        .isEqualTo(expected);
            }
        }
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        Random random = new Random(3);
        String[] zones = {"UTC", "America/Los_Angeles", "+05:30", "-18:00", "Europe/Warsaw", "Etc/GMT+12", "Mars/Olympus_Mons"};
        List<Long> values = new ArrayList<>();
        for (long millis = -2000; millis <= 2000; millis++) {
            values.add(millis);
        }
        values.add(Long.MIN_VALUE);
        values.add(Long.MAX_VALUE);
        for (int i = 0; i < 50_000; i++) {
            values.add(random.nextLong() >> random.nextInt(64));
        }
        for (long epochMillis : values) {
            int picosOfMilli = random.nextInt(1_000_000_000);
            int offsetSeconds = random.nextInt(2 * 18 * 3600 + 1) - 18 * 3600;
            int precision = random.nextInt(13);
            String zone = zones[random.nextInt(zones.length)];
            String expected = expectedDateTime(precision, floorDiv(epochMillis, 1000), floorMod(epochMillis, 1000L) * 1_000_000_000 + picosOfMilli, offsetSeconds) + " " + zone;
            assertThat(formatTimestampWithTimeZone(precision, epochMillis, picosOfMilli, offsetSeconds, zone, zone.getBytes(UTF_8)))
                    .as("millis %s picos %s offset %s precision %s zone %s", epochMillis, picosOfMilli, offsetSeconds, precision, zone)
                    .isEqualTo(expected);
            // the zone id text without its bytes, the path of a zone id that is not ASCII
            assertThat(formatTimestampWithTimeZone(precision, epochMillis, picosOfMilli, offsetSeconds, zone, null)).isEqualTo(expected);
        }
        assertThat(formatTimestampWithTimeZone(3, 0, 0, 0, "Zone/Zürich", null)).isEqualTo("1970-01-01 00:00:00.000 Zone/Zürich");
        assertThat(formatTimestampWithTimeZone(0, Long.MAX_VALUE, 999_999_999, 18 * 3600, "+18:00", "+18:00".getBytes(UTF_8)))
                .isEqualTo(expectedDateTime(0, floorDiv(Long.MAX_VALUE, 1000), 0, 18 * 3600) + " +18:00");
        assertThat(formatTimestampWithTimeZone(12, Long.MIN_VALUE, 0, -18 * 3600, "-18:00", "-18:00".getBytes(UTF_8)))
                .isEqualTo(expectedDateTime(12, floorDiv(Long.MIN_VALUE, 1000), floorMod(Long.MIN_VALUE, 1000L) * 1_000_000_000, -18 * 3600) + " -18:00");
    }

    @Test
    public void testTime()
    {
        List<Long> values = new ArrayList<>();
        for (long picos = 0; picos < 3_000_000_000_000L; picos += 999_999_999_999L / 7) {
            values.add(picos);
        }
        values.add(PICOSECONDS_PER_DAY - 1);
        values.add(PICOSECONDS_PER_DAY - PICOSECONDS_PER_SECOND);
        values.add(12 * 3600 * PICOSECONDS_PER_SECOND);
        Random random = new Random(4);
        for (int i = 0; i < 20_000; i++) {
            values.add(floorMod(random.nextLong(), PICOSECONDS_PER_DAY));
        }
        for (long picos : values) {
            for (int precision = 0; precision <= 12; precision++) {
                String expected = TIME_FORMATTER.format(LocalTime.ofSecondOfDay(picos / PICOSECONDS_PER_SECOND)) + fraction(precision, picos % PICOSECONDS_PER_SECOND);
                assertThat(formatTime(precision, picos)).as("picos %s precision %s", picos, precision).isEqualTo(expected);
                int offsetMinutes = random.nextInt(2 * 14 * 60 + 1) - 14 * 60;
                String offset = String.format("%s%02d:%02d", offsetMinutes < 0 ? "-" : "+", Math.abs(offsetMinutes) / 60, Math.abs(offsetMinutes) % 60);
                assertThat(formatTimeWithTimeZone(precision, picos, offsetMinutes)).as("picos %s precision %s offset %s", picos, precision, offsetMinutes).isEqualTo(expected + offset);
            }
        }
        assertThat(formatTimeWithTimeZone(0, 0, 0)).isEqualTo("00:00:00+00:00");
        assertThat(formatTimeWithTimeZone(0, 0, -1)).isEqualTo("00:00:00-00:01");
        assertThat(formatTimeWithTimeZone(0, 0, -14 * 60)).isEqualTo("00:00:00-14:00");
        assertThat(formatTimeWithTimeZone(0, 0, 14 * 60)).isEqualTo("00:00:00+14:00");
    }

    @Test
    public void testDecimalFromLong()
    {
        List<Long> values = new ArrayList<>();
        for (long value = -1200; value <= 1200; value++) {
            values.add(value);
        }
        for (int digits = 1; digits <= 19; digits++) {
            long power = (long) Math.pow(10, digits - 1);
            values.add(power);
            values.add(-power);
            values.add(power - 1);
            values.add(1 - power);
            values.add(power * 9 - 1);
        }
        values.add(Long.MIN_VALUE);
        values.add(Long.MIN_VALUE + 1);
        values.add(Long.MAX_VALUE);
        values.add(Long.MAX_VALUE - 1);
        Random random = new Random(5);
        for (int i = 0; i < 20_000; i++) {
            values.add(random.nextLong() >> random.nextInt(64));
        }
        for (long unscaled : values) {
            for (int scale = 0; scale <= 40; scale++) {
                String expected = new BigDecimal(BigInteger.valueOf(unscaled), scale).toPlainString();
                assertThat(formatDecimal(unscaled, scale)).as("unscaled %s scale %s", unscaled, scale).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testDecimalFromBytes()
    {
        List<BigInteger> values = new ArrayList<>();
        for (int bits = 0; bits <= 127; bits++) {
            BigInteger power = BigInteger.ONE.shiftLeft(bits);
            values.add(power);
            values.add(power.negate());
            values.add(power.subtract(BigInteger.ONE));
            values.add(power.subtract(BigInteger.ONE).negate());
            values.add(power.add(BigInteger.ONE));
            values.add(power.add(BigInteger.ONE).negate());
        }
        for (int digits = 1; digits <= 39; digits++) {
            BigInteger power = BigInteger.TEN.pow(digits - 1);
            values.add(power);
            values.add(power.negate());
            values.add(power.multiply(BigInteger.TEN).subtract(BigInteger.ONE));
            values.add(power.multiply(BigInteger.TEN).subtract(BigInteger.ONE).negate());
        }
        // the ends of the 128-bit range, and the largest Trino decimals
        values.add(BigInteger.ONE.shiftLeft(127).negate());
        values.add(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE));
        values.add(BigInteger.TEN.pow(38).subtract(BigInteger.ONE));
        values.add(BigInteger.TEN.pow(38).subtract(BigInteger.ONE).negate());
        Random random = new Random(6);
        for (int i = 0; i < 20_000; i++) {
            BigInteger value = new BigInteger(random.nextInt(128), random);
            values.add(random.nextBoolean() ? value : value.negate());
        }
        for (BigInteger unscaled : values) {
            if (unscaled.bitLength() > 127) {
                // outside the two's complement range of 128 bits
                continue;
            }
            for (int scale : new int[] {0, 1, 2, 9, 10, 18, 19, 20, 37, 38, 39, 40, 45}) {
                String expected = new BigDecimal(unscaled, scale).toPlainString();
                assertThat(formatDecimal(unscaled.toByteArray(), scale)).as("unscaled %s scale %s", unscaled, scale).isEqualTo(expected);
                // the same value sign-extended to 16 bytes, the raw form of a 128-bit integer
                assertThat(formatDecimal(signExtend(unscaled, 16), scale)).as("unscaled %s scale %s, 16 bytes", unscaled, scale).isEqualTo(expected);
            }
        }
        // beyond 128 bits, which no Trino decimal reaches, through BigInteger
        BigInteger huge = BigInteger.ONE.shiftLeft(200).add(BigInteger.valueOf(12345));
        assertThat(formatDecimal(huge.toByteArray(), 3)).isEqualTo(new BigDecimal(huge, 3).toPlainString());
        assertThat(formatDecimal(huge.negate().toByteArray(), 0)).isEqualTo(huge.negate().toString());
        assertThat(formatDecimal(signExtend(BigInteger.valueOf(-5), 20), 2)).isEqualTo("-0.05");
    }

    private static byte[] signExtend(BigInteger value, int length)
    {
        byte[] minimal = value.toByteArray();
        byte[] extended = new byte[length];
        if (value.signum() < 0) {
            Arrays.fill(extended, (byte) 0xFF);
        }
        System.arraycopy(minimal, 0, extended, length - minimal.length, minimal.length);
        return extended;
    }

    private static String expectedTimestamp(int precision, long epochMicros, int picosOfMicro, int offsetSeconds)
    {
        return expectedDateTime(precision, floorDiv(epochMicros, 1_000_000), floorMod(epochMicros, 1_000_000L) * 1_000_000 + picosOfMicro, offsetSeconds);
    }

    private static String expectedDateTime(int precision, long epochSecond, long picoFraction, int offsetSeconds)
    {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.ofTotalSeconds(offsetSeconds));
        return DATE_TIME_FORMATTER.format(dateTime) + fraction(precision, picoFraction);
    }

    private static String fraction(int precision, long picoFraction)
    {
        if (precision == 0) {
            return "";
        }
        long scaled = picoFraction / (long) Math.pow(10, 12 - precision);
        return "." + String.format("%0" + precision + "d", scaled);
    }
}
