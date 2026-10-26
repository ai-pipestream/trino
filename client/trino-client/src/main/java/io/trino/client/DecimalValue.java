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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * A {@code decimal} value as a binary encoding carries it: a 128-bit two's complement unscaled value
 * and a scale. It is returned instead of the text of the value when the client asks for typed values
 * (see {@link ClientSession#isTypedValues()}), and {@link #toString()} returns that text, plain
 * notation with exactly {@code scale} fraction digits.
 */
public final class DecimalValue
{
    // nine digits: the magnitude is divided by this until it fits in a long
    private static final long DECIMAL_WORD = 1_000_000_000L;
    // "00" to "99"
    private static final byte[] DIGIT_PAIRS = new byte[200];

    static {
        for (int i = 0; i < 100; i++) {
            DIGIT_PAIRS[2 * i] = (byte) ('0' + i / 10);
            DIGIT_PAIRS[2 * i + 1] = (byte) ('0' + i % 10);
        }
    }

    private final long unscaledHigh;
    private final long unscaledLow;
    private final int scale;

    public DecimalValue(long unscaledHigh, long unscaledLow, int scale)
    {
        checkArgument(scale >= 0, "scale is negative: %s", scale);
        this.unscaledHigh = unscaledHigh;
        this.unscaledLow = unscaledLow;
        this.scale = scale;
    }

    public static DecimalValue of(long unscaled, int scale)
    {
        return new DecimalValue(unscaled >> 63, unscaled, scale);
    }

    public int getScale()
    {
        return scale;
    }

    public BigDecimal toBigDecimal()
    {
        if (unscaledHigh == unscaledLow >> 63) {
            return BigDecimal.valueOf(unscaledLow, scale);
        }
        byte[] bytes = new byte[2 * Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++) {
            bytes[i] = (byte) (unscaledHigh >>> (8 * (Long.BYTES - 1 - i)));
            bytes[Long.BYTES + i] = (byte) (unscaledLow >>> (8 * (Long.BYTES - 1 - i)));
        }
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DecimalValue)) {
            return false;
        }
        DecimalValue that = (DecimalValue) o;
        return unscaledHigh == that.unscaledHigh &&
                unscaledLow == that.unscaledLow &&
                scale == that.scale;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unscaledHigh, unscaledLow, scale);
    }

    /**
     * The text of the value, as {@code BigDecimal.toPlainString()} renders it, without a {@code BigDecimal}:
     * {@code getString} of a decimal column pays for this on every value.
     */
    @Override
    public String toString()
    {
        boolean negative = unscaledHigh < 0;
        long high = unscaledHigh;
        long low = unscaledLow;
        if (negative) {
            low = -low;
            high = ~high + (low == 0 ? 1 : 0);
        }
        // the unsigned magnitude as four 32-bit words, most significant first, divided by 10^9 until
        // it fits in a non-negative long; each remainder is nine digits, and 2^128 is below 10^39, so
        // there are at most three
        long word0 = high >>> 32;
        long word1 = high & 0xFFFF_FFFFL;
        long word2 = low >>> 32;
        long word3 = low & 0xFFFF_FFFFL;
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
        // positive after a division, as the magnitude was at least 2^63
        long quotient = (word2 << 32) | word3;
        int digitCount = 9 * remainders + digitCount(quotient);

        int sign = negative ? 1 : 0;
        int length = sign + (digitCount <= scale ? scale + 2 : digitCount + (scale > 0 ? 1 : 0));
        byte[] text = new byte[length];
        // the digits end the text, the remainders padded to nine digits
        int index = length;
        if (remainders > 0) {
            index = writeDigits(text, index, remainder0, 9);
        }
        if (remainders > 1) {
            index = writeDigits(text, index, remainder1, 9);
        }
        if (remainders > 2) {
            index = writeDigits(text, index, remainder2, 9);
        }
        index = writeDigits(text, index, quotient, digitCount - 9 * remainders);
        if (negative) {
            text[0] = '-';
        }
        if (digitCount <= scale) {
            // 0.000ddd
            text[sign] = '0';
            text[sign + 1] = '.';
            for (int i = sign + 2; i < index; i++) {
                text[i] = '0';
            }
        }
        else if (scale > 0) {
            // the integer digits move left, over the free byte before them, to make room for the point
            int integerDigits = digitCount - scale;
            System.arraycopy(text, sign + 1, text, sign, integerDigits);
            text[sign + integerDigits] = '.';
        }
        return new String(text, ISO_8859_1);
    }

    /**
     * Writes the {@code digits} digits of the non-negative {@code value}, padded with zeros, so that they
     * end before {@code end}, and returns the index of the first.
     */
    private static int writeDigits(byte[] text, int end, long value, int digits)
    {
        int start = end - digits;
        while (end - start >= 2) {
            int pair = (int) (value % 100);
            value /= 100;
            text[--end] = DIGIT_PAIRS[2 * pair + 1];
            text[--end] = DIGIT_PAIRS[2 * pair];
        }
        if (end > start) {
            text[--end] = (byte) ('0' + value);
        }
        return start;
    }

    private static int digitCount(long value)
    {
        int digits = 1;
        long limit = 10;
        // 10^18 is the largest power of ten below Long.MAX_VALUE
        while (digits < 19 && value >= limit) {
            digits++;
            limit *= 10;
        }
        return digits;
    }
}
