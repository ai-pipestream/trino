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
package io.trino.client;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

/**
 * A {@code timestamp} or {@code timestamp with time zone} value as the fields of its text: the local
 * date-time, the fraction of the second in picoseconds, the precision and, for a
 * {@code timestamp with time zone}, the zone id. It is returned instead of the text of the value when
 * the client asks for typed values (see {@link ClientSession#isTypedValues()}), and
 * {@link #toString()} returns that text, such as {@code 2020-09-13 12:26:40.123 Europe/Warsaw}.
 */
public final class TimestampValue
{
    private static final int MAX_PRECISION = 12;
    private static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
    };
    // "00" to "99"
    private static final byte[] DIGIT_PAIRS = new byte[200];

    static {
        for (int i = 0; i < 100; i++) {
            DIGIT_PAIRS[2 * i] = (byte) ('0' + i / 10);
            DIGIT_PAIRS[2 * i + 1] = (byte) ('0' + i % 10);
        }
    }

    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final int second;
    private final long picosOfSecond;
    private final int precision;
    private final Optional<String> zoneId;

    public TimestampValue(int year, int month, int day, int hour, int minute, int second, long picosOfSecond, int precision, Optional<String> zoneId)
    {
        checkArgument(month >= 1 && month <= 12, "invalid month: %s", month);
        checkArgument(day >= 1 && day <= 31, "invalid day: %s", day);
        checkArgument(hour >= 0 && hour <= 23, "invalid hour: %s", hour);
        checkArgument(minute >= 0 && minute <= 59, "invalid minute: %s", minute);
        checkArgument(second >= 0 && second <= 59, "invalid second: %s", second);
        checkArgument(precision >= 0 && precision <= MAX_PRECISION, "invalid precision: %s", precision);
        checkArgument(picosOfSecond >= 0 && picosOfSecond < POWERS_OF_TEN[MAX_PRECISION], "invalid picos of second: %s", picosOfSecond);
        checkArgument(picosOfSecond % POWERS_OF_TEN[MAX_PRECISION - precision] == 0, "picos of second %s have more digits than precision %s", picosOfSecond, precision);
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.picosOfSecond = picosOfSecond;
        this.precision = precision;
        this.zoneId = requireNonNull(zoneId, "zoneId is null");
    }

    public int getYear()
    {
        return year;
    }

    public int getMonth()
    {
        return month;
    }

    public int getDay()
    {
        return day;
    }

    public int getHour()
    {
        return hour;
    }

    public int getMinute()
    {
        return minute;
    }

    public int getSecond()
    {
        return second;
    }

    /**
     * The fraction of the second, in picoseconds, with no more digits than the precision.
     */
    public long getPicosOfSecond()
    {
        return picosOfSecond;
    }

    public int getPrecision()
    {
        return precision;
    }

    /**
     * The zone id of a {@code timestamp with time zone}, empty for a {@code timestamp}.
     */
    public Optional<String> getZoneId()
    {
        return zoneId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TimestampValue)) {
            return false;
        }
        TimestampValue that = (TimestampValue) o;
        return year == that.year &&
                month == that.month &&
                day == that.day &&
                hour == that.hour &&
                minute == that.minute &&
                second == that.second &&
                picosOfSecond == that.picosOfSecond &&
                precision == that.precision &&
                zoneId.equals(that.zoneId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(year, month, day, hour, minute, second, picosOfSecond, precision, zoneId);
    }

    /**
     * Formats like {@code SqlTimestamp} and {@code SqlTimestampWithTimeZone}: {@code uuuu-MM-dd HH:mm:ss}
     * as {@code DateTimeFormatter} renders it, {@code precision} fraction digits after a point when
     * {@code precision} is positive, and a space and the zone id for a {@code timestamp with time zone}.
     * {@code getString} of a timestamp column pays for this on every value, so it writes Latin-1 bytes.
     */
    @Override
    public String toString()
    {
        // uuuu: at least four digits, a minus sign for a negative year and a plus sign beyond 9999
        int yearDigits = 4;
        while (yearDigits < 10 && Math.abs((long) year) >= POWERS_OF_TEN[yearDigits]) {
            yearDigits++;
        }
        boolean signed = year < 0 || year > 9999;
        String zone = zoneId.orElse(null);
        boolean latin1Zone = zone == null || isLatin1(zone);
        int length = (signed ? 1 : 0) + yearDigits + 15 + (precision > 0 ? precision + 1 : 0) + (zone != null && latin1Zone ? zone.length() + 1 : 0);
        byte[] text = new byte[length];
        int index = 0;
        if (signed) {
            text[index++] = (byte) (year < 0 ? '-' : '+');
        }
        index = writeDigits(text, index, Math.abs((long) year), yearDigits);
        text[index++] = '-';
        index = writeDigits(text, index, month, 2);
        text[index++] = '-';
        index = writeDigits(text, index, day, 2);
        text[index++] = ' ';
        index = writeDigits(text, index, hour, 2);
        text[index++] = ':';
        index = writeDigits(text, index, minute, 2);
        text[index++] = ':';
        index = writeDigits(text, index, second, 2);
        if (precision > 0) {
            text[index++] = '.';
            index = writeDigits(text, index, picosOfSecond / POWERS_OF_TEN[MAX_PRECISION - precision], precision);
        }
        if (zone == null) {
            return new String(text, ISO_8859_1);
        }
        if (!latin1Zone) {
            return new String(text, ISO_8859_1) + ' ' + zone;
        }
        text[index++] = ' ';
        for (int i = 0; i < zone.length(); i++) {
            text[index++] = (byte) zone.charAt(i);
        }
        return new String(text, ISO_8859_1);
    }

    /**
     * Writes {@code value} as {@code digits} digits, padded with zeros, and returns the index after them.
     */
    private static int writeDigits(byte[] text, int index, long value, int digits)
    {
        int end = index + digits;
        while (end - index >= 2) {
            int pair = (int) (value % 100);
            value /= 100;
            text[--end] = DIGIT_PAIRS[2 * pair + 1];
            text[--end] = DIGIT_PAIRS[2 * pair];
        }
        if (end > index) {
            text[--end] = (byte) ('0' + value);
        }
        return index + digits;
    }

    private static boolean isLatin1(String value)
    {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0xFF) {
                return false;
            }
        }
        return true;
    }
}
