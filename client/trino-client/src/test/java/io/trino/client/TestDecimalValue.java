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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDecimalValue
{
    private static final BigInteger MAX_128 = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
    private static final BigInteger MIN_128 = BigInteger.ONE.shiftLeft(127).negate();

    @Test
    public void testMatchesBigDecimal()
    {
        List<BigInteger> unscaledValues = new ArrayList<>();
        for (BigInteger value : new BigInteger[] {
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(Long.MIN_VALUE),
                BigInteger.ONE.shiftLeft(63),
                BigInteger.ONE.shiftLeft(64),
                BigInteger.TEN.pow(18),
                BigInteger.TEN.pow(19),
                BigInteger.TEN.pow(27),
                BigInteger.TEN.pow(36),
                BigInteger.TEN.pow(38).subtract(BigInteger.ONE),
                MAX_128,
        }) {
            unscaledValues.add(value);
            unscaledValues.add(value.negate().max(MIN_128));
            unscaledValues.add(value.subtract(BigInteger.ONE).max(MIN_128));
        }
        unscaledValues.add(MIN_128);
        Random random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            BigInteger value = new BigInteger(random.nextInt(128), random);
            unscaledValues.add(random.nextBoolean() ? value : value.negate());
        }

        for (BigInteger unscaled : unscaledValues) {
            for (int scale = 0; scale <= 40; scale++) {
                DecimalValue value = new DecimalValue(unscaled.shiftRight(Long.SIZE).longValue(), unscaled.longValue(), scale);
                BigDecimal expected = new BigDecimal(unscaled, scale);
                assertThat(value.toBigDecimal()).isEqualTo(expected);
                assertThat(value.toString()).as("%s scale %s", unscaled, scale).isEqualTo(expected.toPlainString());
            }
        }
    }

    @Test
    public void testOf()
    {
        assertThat(DecimalValue.of(-12345, 2)).isEqualTo(new DecimalValue(-1, -12345, 2));
        assertThat(DecimalValue.of(-12345, 2).toString()).isEqualTo("-123.45");
        assertThat(DecimalValue.of(5, 3).toString()).isEqualTo("0.005");
        assertThat(DecimalValue.of(0, 2).toString()).isEqualTo("0.00");
        assertThat(DecimalValue.of(0, 0).toString()).isEqualTo("0");
    }
}
