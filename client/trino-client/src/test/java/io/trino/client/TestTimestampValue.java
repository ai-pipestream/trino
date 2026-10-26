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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestampValue
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    @Test
    public void testMatchesDateTimeFormatter()
    {
        Random random = new Random(42);
        for (int i = 0; i < 50_000; i++) {
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(random.nextLong() % 400_000_000_000L, 0, ZoneOffset.UTC);
            int precision = random.nextInt(13);
            long unit = (long) Math.pow(10, 12 - precision);
            long picos = Math.floorMod(random.nextLong(), 1_000_000_000_000L) / unit * unit;
            Optional<String> zone = random.nextBoolean() ? Optional.empty() : Optional.of(random.nextBoolean() ? "Europe/Warsaw" : "+05:30");
            TimestampValue value = new TimestampValue(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), picos, precision, zone);

            StringBuilder expected = new StringBuilder(FORMATTER.format(dateTime));
            if (precision > 0) {
                expected.append('.').append(String.format("%0" + precision + "d", picos / unit));
            }
            zone.ifPresent(id -> expected.append(' ').append(id));
            assertThat(value.toString()).isEqualTo(expected.toString());
        }
    }

    @Test
    public void testToString()
    {
        assertThat(new TimestampValue(2020, 9, 13, 12, 26, 40, 123_000_000_000L, 3, Optional.of("Europe/Warsaw")).toString()).isEqualTo("2020-09-13 12:26:40.123 Europe/Warsaw");
        assertThat(new TimestampValue(5, 1, 2, 3, 4, 5, 0, 0, Optional.empty()).toString()).isEqualTo("0005-01-02 03:04:05");
        assertThat(new TimestampValue(-1, 12, 31, 23, 59, 59, 1, 12, Optional.empty()).toString()).isEqualTo("-0001-12-31 23:59:59.000000000001");
        assertThat(new TimestampValue(10000, 1, 1, 0, 0, 0, 0, 1, Optional.empty()).toString()).isEqualTo("+10000-01-01 00:00:00.0");
        assertThat(new TimestampValue(Integer.MIN_VALUE, 1, 1, 0, 0, 0, 0, 0, Optional.empty()).toString()).isEqualTo("-2147483648-01-01 00:00:00");
        // a zone id beyond Latin-1 is appended as text
        assertThat(new TimestampValue(2020, 1, 1, 0, 0, 0, 0, 0, Optional.of("Zone/Łódź")).toString()).isEqualTo("2020-01-01 00:00:00 Zone/Łódź");
        assertThat(new TimestampValue(2020, 1, 1, 0, 0, 0, 0, 0, Optional.of("Zone/é")).toString()).isEqualTo("2020-01-01 00:00:00 Zone/é");
    }

    @Test
    public void testPrecision()
    {
        assertThatThrownBy(() -> new TimestampValue(2020, 1, 1, 0, 0, 0, 1_000_000_000L, 2, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more digits than precision");
    }
}
