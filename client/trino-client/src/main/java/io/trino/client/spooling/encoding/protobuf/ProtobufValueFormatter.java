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

import io.trino.client.TimestampValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Formats the temporal and decimal values of the protobuf encoding as the text the json encoding
 * carries, which is what the client API exposes. The json encoding formats on the server through
 * {@code SqlDecimal}, {@code SqlDate}, {@code SqlTime}, {@code SqlTimestamp} and their time zone
 * variants in trino-spi, which the client cannot depend on; the formats here are pinned to them by
 * tests.
 * <p>
 * The text is written into a buffer the formatter reuses and turned into a Latin-1 string, so a value
 * costs one allocation, the string itself. This is the hot path of a result with many decimals or
 * timestamps, where the json decoder gets the text preformatted.
 * <p>
 * Instances are not thread-safe.
 */
final class ProtobufValueFormatter
{
    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long MILLISECONDS_PER_SECOND = 1_000;
    private static final long PICOSECONDS_PER_MICROSECOND = 1_000_000;
    private static final long PICOSECONDS_PER_MILLISECOND = 1_000_000_000;
    private static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    private static final int SECONDS_PER_DAY = 24 * 60 * 60;
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
            10_000_000_000_000L,
            100_000_000_000_000L,
            1_000_000_000_000_000L,
            10_000_000_000_000_000L,
            100_000_000_000_000_000L,
            1_000_000_000_000_000_000L,
    };
    // nine digits of a 128-bit decimal: the magnitude is divided by this until it fits in a long
    private static final long DECIMAL_WORD = 1_000_000_000L;
    // 2^128 has 39 digits
    private static final int DIGITS_LENGTH = 39;
    // "00" to "99"
    private static final byte[] DIGIT_PAIRS = new byte[200];
    private static final int INITIAL_BUFFER_SIZE = 64;

    static {
        for (int i = 0; i < 100; i++) {
            DIGIT_PAIRS[2 * i] = (byte) ('0' + i / 10);
            DIGIT_PAIRS[2 * i + 1] = (byte) ('0' + i % 10);
        }
    }

    private byte[] buffer = new byte[INITIAL_BUFFER_SIZE];
    // the digits of a decimal, written backwards from the end before they are laid out in the buffer
    private final byte[] digits = new byte[DIGITS_LENGTH];

    /**
     * Formats like {@code LocalDate.toString()}: {@code uuuu-MM-dd}, a year beyond 9999 with a plus
     * sign, a negative year with a minus sign, both padded to four digits.
     */
    String formatDate(long epochDay)
    {
        long date = civilDate(epochDay);
        int year = yearOf(date);
        int yearLength = yearLength(year);
        int length = yearLength + 6;
        byte[] buffer = buffer(length);
        int index = writeYear(buffer, year, yearLength);
        buffer[index++] = '-';
        index = writeTwoDigits(buffer, index, monthOf(date));
        buffer[index++] = '-';
        writeTwoDigits(buffer, index, dayOf(date));
        return latin1(buffer, length);
    }

    /**
     * Formats like {@code SqlTime}: {@code HH:mm:ss}, followed by {@code precision} fraction digits
     * after a point when {@code precision} is positive.
     */
    String formatTime(int precision, long picos)
    {
        int length = 8 + fractionLength(precision);
        byte[] buffer = buffer(length);
        writeTime(buffer, 0, precision, picos);
        return latin1(buffer, length);
    }

    /**
     * Formats like {@code SqlTimeWithTimeZone}: the time followed by the offset as {@code +HH:mm}.
     */
    String formatTimeWithTimeZone(int precision, long picos, int offsetMinutes)
    {
        int length = 8 + fractionLength(precision) + 6;
        byte[] buffer = buffer(length);
        int index = writeTime(buffer, 0, precision, picos);
        writeOffset(buffer, index, offsetMinutes);
        return latin1(buffer, length);
    }

    /**
     * Formats like {@code SqlTimestamp}: {@code uuuu-MM-dd HH:mm:ss} as {@code DateTimeFormatter}
     * renders it, followed by {@code precision} fraction digits after a point when {@code precision}
     * is positive.
     */
    String formatTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        long picoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        long date = civilDate(floorDiv(epochSecond, SECONDS_PER_DAY));
        int length = dateTimeLength(date, precision);
        byte[] buffer = buffer(length);
        writeDateTime(buffer, date, epochSecond, picoFraction, precision);
        return latin1(buffer, length);
    }

    /**
     * The fields of the text {@link #formatTimestamp} returns.
     */
    static TimestampValue timestampValue(int precision, long epochMicros, int picosOfMicro)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        long picoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        return timestampValue(epochSecond, picoFraction, precision, Optional.empty());
    }

    /**
     * The fields of the text {@link #formatTimestampWithTimeZone} returns.
     */
    static TimestampValue timestampWithTimeZoneValue(int precision, long epochMillis, int picosOfMilli, int offsetSeconds, Optional<String> zoneId)
    {
        long epochSecond = floorDiv(epochMillis, MILLISECONDS_PER_SECOND) + offsetSeconds;
        long picoFraction = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;
        return timestampValue(epochSecond, picoFraction, precision, zoneId);
    }

    private static TimestampValue timestampValue(long epochSecond, long picoFraction, int precision, Optional<String> zoneId)
    {
        long date = civilDate(floorDiv(epochSecond, SECONDS_PER_DAY));
        int secondOfDay = (int) floorMod(epochSecond, SECONDS_PER_DAY);
        // the text has precision digits of the fraction
        long picosOfSecond = picoFraction - picoFraction % POWERS_OF_TEN[MAX_PRECISION - precision];
        return new TimestampValue(
                yearOf(date),
                monthOf(date),
                dayOf(date),
                secondOfDay / 3600,
                (secondOfDay / 60) % 60,
                secondOfDay % 60,
                picosOfSecond,
                precision,
                zoneId);
    }

    /**
     * Formats like {@code SqlTimestampWithTimeZone}: the local date-time at {@code offsetSeconds}
     * followed by a space and the zone id. The zone id is given as text and, when it is ASCII, as the
     * bytes of that text, so a cached zone id costs no conversion.
     */
    String formatTimestampWithTimeZone(int precision, long epochMillis, int picosOfMilli, int offsetSeconds, String zoneId, byte[] asciiZoneId)
    {
        long epochSecond = floorDiv(epochMillis, MILLISECONDS_PER_SECOND) + offsetSeconds;
        long picoFraction = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;
        long date = civilDate(floorDiv(epochSecond, SECONDS_PER_DAY));
        int dateTimeLength = dateTimeLength(date, precision);
        if (asciiZoneId == null) {
            byte[] buffer = buffer(dateTimeLength);
            writeDateTime(buffer, date, epochSecond, picoFraction, precision);
            return latin1(buffer, dateTimeLength) + " " + zoneId;
        }
        int length = dateTimeLength + 1 + asciiZoneId.length;
        byte[] buffer = buffer(length);
        int index = writeDateTime(buffer, date, epochSecond, picoFraction, precision);
        buffer[index++] = ' ';
        System.arraycopy(asciiZoneId, 0, buffer, index, asciiZoneId.length);
        return latin1(buffer, length);
    }

    /**
     * Formats like {@code SqlDecimal}: plain notation with exactly {@code scale} fraction digits, and
     * a zero before the point when the value is below one.
     */
    String formatDecimal(long unscaled, int scale)
    {
        if (unscaled == Long.MIN_VALUE) {
            // its magnitude does not fit in a long
            return formatDecimal(-1, unscaled, scale);
        }
        boolean negative = unscaled < 0;
        int start = writeDigits(digits, DIGITS_LENGTH, Math.abs(unscaled));
        return formatDecimal(negative, start, scale);
    }

    /**
     * Formats the unscaled value given as big-endian two's complement bytes, as {@code BigInteger}
     * writes it. A value of up to 128 bits, which covers every Trino decimal, is converted without
     * a {@code BigInteger}.
     */
    String formatDecimal(byte[] unscaled, int scale)
    {
        if (unscaled.length > 16) {
            return new BigDecimal(new BigInteger(unscaled), scale).toPlainString();
        }
        long high = unscaled[0] < 0 ? -1 : 0;
        long low = high;
        for (byte b : unscaled) {
            high = (high << 8) | (low >>> 56);
            low = (low << 8) | (b & 0xFF);
        }
        return formatDecimal(high, low, scale);
    }

    /**
     * Formats the 128-bit two's complement unscaled value {@code high:low}.
     */
    String formatDecimal(long high, long low, int scale)
    {
        boolean negative = high < 0;
        if (negative) {
            low = -low;
            high = ~high + (low == 0 ? 1 : 0);
        }
        // the unsigned magnitude as four 32-bit words, most significant first
        long word0 = high >>> 32;
        long word1 = high & 0xFFFF_FFFFL;
        long word2 = low >>> 32;
        long word3 = low & 0xFFFF_FFFFL;
        // Divide by 10^9 until the quotient fits in a non-negative long; each remainder is nine
        // digits. Every division is by a constant, which the compiler turns into a multiplication.
        // 2^128 is below 10^39, so at most three remainders are taken
        int remainders = 0;
        long remainder0 = 0;
        long remainder1 = 0;
        long remainder2 = 0;
        while (word0 != 0 || word1 != 0 || word2 > Integer.MAX_VALUE) {
            long remainder = word0 % DECIMAL_WORD;
            word0 /= DECIMAL_WORD;
            long current = (remainder << 32) | word1;
            word1 = current / DECIMAL_WORD;
            remainder = current % DECIMAL_WORD;
            current = (remainder << 32) | word2;
            word2 = current / DECIMAL_WORD;
            remainder = current % DECIMAL_WORD;
            current = (remainder << 32) | word3;
            word3 = current / DECIMAL_WORD;
            remainder = current % DECIMAL_WORD;
            if (remainders == 0) {
                remainder0 = remainder;
            }
            else if (remainders == 1) {
                remainder1 = remainder;
            }
            else {
                remainder2 = remainder;
            }
            remainders++;
        }
        long quotient = (word2 << 32) | word3;
        if (remainders == 0) {
            return formatDecimal(negative, writeDigits(digits, DIGITS_LENGTH, quotient), scale);
        }
        // the remainders are the least significant words, each padded to nine digits
        int end = DIGITS_LENGTH;
        end = writeNineDigits(digits, end, (int) remainder0);
        if (remainders > 1) {
            end = writeNineDigits(digits, end, (int) remainder1);
        }
        if (remainders > 2) {
            end = writeNineDigits(digits, end, (int) remainder2);
        }
        return formatDecimal(negative, writeDigits(digits, end, quotient), scale);
    }

    /**
     * Lays out the digits in {@code digits[start, DIGITS_LENGTH)}, which have no leading zero, with
     * the sign, the point and the zeros the scale needs.
     */
    private String formatDecimal(boolean negative, int start, int scale)
    {
        int digitCount = DIGITS_LENGTH - start;
        int length = decimalLength(negative, digitCount, scale);
        byte[] buffer = buffer(length);
        int index = 0;
        if (negative) {
            buffer[index++] = '-';
        }
        if (digitCount <= scale) {
            buffer[index++] = '0';
        }
        else {
            System.arraycopy(digits, start, buffer, index, digitCount - scale);
            index += digitCount - scale;
        }
        if (scale > 0) {
            buffer[index++] = '.';
            for (int i = digitCount; i < scale; i++) {
                buffer[index++] = '0';
            }
            System.arraycopy(digits, start + Math.max(0, digitCount - scale), buffer, index, Math.min(digitCount, scale));
        }
        return latin1(buffer, length);
    }

    /**
     * Writes the digits of the non-negative {@code value}, without leading zeros, so that they end before
     * {@code end}, and returns the index of the first digit.
     */
    private static int writeDigits(byte[] buffer, int end, long value)
    {
        while (value >= 100) {
            int pair = (int) (value % 100);
            value /= 100;
            buffer[--end] = DIGIT_PAIRS[2 * pair + 1];
            buffer[--end] = DIGIT_PAIRS[2 * pair];
        }
        if (value >= 10) {
            buffer[--end] = DIGIT_PAIRS[2 * (int) value + 1];
            buffer[--end] = DIGIT_PAIRS[2 * (int) value];
        }
        else {
            buffer[--end] = (byte) ('0' + value);
        }
        return end;
    }

    /**
     * Writes {@code value}, below 10^9, as nine digits ending before {@code end}, and returns the index
     * of the first digit.
     */
    private static int writeNineDigits(byte[] buffer, int end, int value)
    {
        for (int i = 0; i < 4; i++) {
            int pair = value % 100;
            value /= 100;
            buffer[--end] = DIGIT_PAIRS[2 * pair + 1];
            buffer[--end] = DIGIT_PAIRS[2 * pair];
        }
        buffer[--end] = (byte) ('0' + value);
        return end;
    }

    private static int decimalLength(boolean negative, int digits, int scale)
    {
        return (negative ? 1 : 0) + (digits <= scale ? 1 : digits - scale) + (scale > 0 ? scale + 1 : 0);
    }

    private static int digitsOfNegative(long value)
    {
        for (int digits = 1; digits < POWERS_OF_TEN.length; digits++) {
            if (value > -POWERS_OF_TEN[digits]) {
                return digits;
            }
        }
        return POWERS_OF_TEN.length;
    }

    private static int dateTimeLength(long date, int precision)
    {
        return yearLength(yearOf(date)) + 15 + fractionLength(precision);
    }

    private static int fractionLength(int precision)
    {
        return precision == 0 ? 0 : precision + 1;
    }

    /**
     * Writes {@code uuuu-MM-dd HH:mm:ss} and the fraction, and returns the index after them. The date is
     * the packed civil date of the day of {@code epochSecond}.
     */
    private static int writeDateTime(byte[] buffer, long date, long epochSecond, long picoFraction, int precision)
    {
        int secondOfDay = (int) floorMod(epochSecond, SECONDS_PER_DAY);
        int year = yearOf(date);
        int index = writeYear(buffer, year, yearLength(year));
        buffer[index++] = '-';
        index = writeTwoDigits(buffer, index, monthOf(date));
        buffer[index++] = '-';
        index = writeTwoDigits(buffer, index, dayOf(date));
        buffer[index++] = ' ';
        index = writeTwoDigits(buffer, index, secondOfDay / 3600);
        buffer[index++] = ':';
        index = writeTwoDigits(buffer, index, (secondOfDay / 60) % 60);
        buffer[index++] = ':';
        index = writeTwoDigits(buffer, index, secondOfDay % 60);
        return writeFraction(buffer, index, precision, picoFraction);
    }

    private static int writeTime(byte[] buffer, int index, int precision, long picos)
    {
        int secondOfDay = (int) (picos / PICOSECONDS_PER_SECOND);
        index = writeTwoDigits(buffer, index, secondOfDay / 3600);
        buffer[index++] = ':';
        index = writeTwoDigits(buffer, index, (secondOfDay / 60) % 60);
        buffer[index++] = ':';
        index = writeTwoDigits(buffer, index, secondOfDay % 60);
        return writeFraction(buffer, index, precision, picos % PICOSECONDS_PER_SECOND);
    }

    private static int writeFraction(byte[] buffer, int index, int precision, long picoFraction)
    {
        if (precision == 0) {
            return index;
        }
        buffer[index++] = '.';
        long fraction = picoFraction / POWERS_OF_TEN[MAX_PRECISION - precision];
        for (int i = index + precision - 1; i >= index; i--) {
            buffer[i] = (byte) ('0' + (fraction % 10));
            fraction /= 10;
        }
        return index + precision;
    }

    private static void writeOffset(byte[] buffer, int index, int offsetMinutes)
    {
        buffer[index++] = (byte) (offsetMinutes >= 0 ? '+' : '-');
        int magnitude = Math.abs(offsetMinutes);
        index = writeTwoDigits(buffer, index, magnitude / 60);
        buffer[index++] = ':';
        writeTwoDigits(buffer, index, magnitude % 60);
    }

    private static int yearLength(int year)
    {
        if (year < 0) {
            return 1 + Math.max(4, digitsOfNegative(year));
        }
        if (year > 9999) {
            return 1 + digitsOfNegative(-year);
        }
        return 4;
    }

    /**
     * Writes the year as {@code uuuu} of {@code DateTimeFormatter} and {@code LocalDate.toString()}
     * do: at least four digits, a minus sign for a negative year and a plus sign beyond 9999.
     */
    private static int writeYear(byte[] buffer, int year, int yearLength)
    {
        int index = 0;
        if (year < 0) {
            buffer[index++] = '-';
        }
        else if (year > 9999) {
            buffer[index++] = '+';
        }
        // negative, which has room for Integer.MIN_VALUE
        int magnitude = year < 0 ? year : -year;
        for (int i = yearLength - 1; i >= index; i--) {
            buffer[i] = (byte) ('0' - (magnitude % 10));
            magnitude /= 10;
        }
        return yearLength;
    }

    private static int writeTwoDigits(byte[] buffer, int index, int value)
    {
        buffer[index] = (byte) ('0' + value / 10);
        buffer[index + 1] = (byte) ('0' + value % 10);
        return index + 2;
    }

    /**
     * The proleptic Gregorian year, month and day of a day since 1970-01-01, packed as
     * {@code year << 16 | month << 8 | day}. The year is in the range of {@code LocalDate}, and the
     * day is in the range of Trino's temporal types.
     */
    private static long civilDate(long epochDay)
    {
        // Days since 0000-03-01, in eras of 400 years (146,097 days); the year starts in March so
        // that the leap day is the last day of the year
        long shifted = epochDay + 719_468;
        long era = floorDiv(shifted, 146_097);
        long dayOfEra = shifted - era * 146_097;
        long yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365;
        long dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
        long shiftedMonth = (5 * dayOfYear + 2) / 153;
        long day = dayOfYear - (153 * shiftedMonth + 2) / 5 + 1;
        long month = shiftedMonth < 10 ? shiftedMonth + 3 : shiftedMonth - 9;
        long year = yearOfEra + era * 400 + (month <= 2 ? 1 : 0);
        return (year << 16) | (month << 8) | day;
    }

    private static int yearOf(long packed)
    {
        return (int) (packed >> 16);
    }

    private static int monthOf(long packed)
    {
        return (int) ((packed >> 8) & 0xFF);
    }

    private static int dayOf(long packed)
    {
        return (int) (packed & 0xFF);
    }

    /**
     * Returns the reusable buffer, grown to at least {@code length} bytes.
     */
    private byte[] buffer(int length)
    {
        if (buffer.length < length) {
            buffer = new byte[Math.max(length, 2 * buffer.length)];
        }
        return buffer;
    }

    private static String latin1(byte[] buffer, int length)
    {
        return new String(buffer, 0, length, ISO_8859_1);
    }
}
