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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.DecimalValue;
import io.trino.client.TimestampValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TimeZone;

import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads typed values (see {@link io.trino.client.ClientSession#isTypedValues()}) and their text through
 * every getter of a result set, and checks that each returns the same value or throws the same exception.
 */
public class TestTypedValues
{
    private static final int ROWS = 2_000;
    private static final List<String> ZONES = ImmutableList.of("UTC", "Europe/Warsaw", "America/Los_Angeles", "Asia/Kathmandu", "Australia/Lord_Howe", "+05:30", "-08:00");
    private static final List<Class<?>> TARGETS = ImmutableList.of(
            String.class,
            Object.class,
            BigDecimal.class,
            Number.class,
            Long.class,
            Date.class,
            LocalDate.class,
            LocalDateTime.class,
            Time.class,
            Timestamp.class,
            Instant.class,
            ZonedDateTime.class,
            OffsetDateTime.class);

    @Test
    public void testGettersMatchText()
            throws SQLException
    {
        Random random = new Random(42);
        List<Column> columns = new ArrayList<>();
        List<ValueGenerator> generators = new ArrayList<>();
        for (int[] precisionAndScale : new int[][] {{3, 0}, {10, 2}, {18, 18}, {19, 4}, {38, 0}, {38, 10}}) {
            columns.add(column(DECIMAL, precisionAndScale[0], precisionAndScale[1]));
            generators.add(() -> decimal(random, precisionAndScale[0], precisionAndScale[1]));
        }
        columns.add(column(DATE));
        generators.add(() -> date(random));
        for (int precision : new int[] {0, 3, 6, 9, 12}) {
            columns.add(column(TIMESTAMP, precision));
            generators.add(() -> timestamp(random, precision, Optional.empty()));
            columns.add(column(TIMESTAMP_WITH_TIME_ZONE, precision));
            generators.add(() -> timestamp(random, precision, Optional.of(ZONES.get(random.nextInt(ZONES.size())))));
        }

        List<List<Object>> typedRows = new ArrayList<>();
        List<List<Object>> textRows = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            List<Object> typedRow = new ArrayList<>();
            List<Object> textRow = new ArrayList<>();
            for (ValueGenerator generator : generators) {
                Object value = random.nextInt(50) == 0 ? null : generator.next();
                typedRow.add(value);
                textRow.add(value == null ? null : value.toString());
            }
            typedRows.add(typedRow);
            textRows.add(textRow);
        }

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Asia/Kathmandu")));
        try (ResultSet typed = new InMemoryTrinoResultSet(columns, typedRows);
                ResultSet text = new InMemoryTrinoResultSet(columns, textRows)) {
            for (int row = 0; row < ROWS; row++) {
                assertThat(typed.next()).isTrue();
                assertThat(text.next()).isTrue();
                for (int column = 1; column <= columns.size(); column++) {
                    int index = column;
                    String description = String.format("row %s, column %s (%s): %s", row, column, columns.get(column - 1).getType(), textRows.get(row).get(column - 1));
                    assertSame(description + " getString", typed, text, resultSet -> resultSet.getString(index));
                    assertSame(description + " getObject", typed, text, resultSet -> resultSet.getObject(index));
                    for (Class<?> target : TARGETS) {
                        assertSame(description + " getObject " + target.getSimpleName(), typed, text, resultSet -> resultSet.getObject(index, target));
                    }
                    assertSame(description + " getBigDecimal", typed, text, resultSet -> resultSet.getBigDecimal(index));
                    assertSame(description + " getLong", typed, text, resultSet -> resultSet.getLong(index));
                    assertSame(description + " getInt", typed, text, resultSet -> resultSet.getInt(index));
                    assertSame(description + " getDouble", typed, text, resultSet -> resultSet.getDouble(index));
                    assertSame(description + " getBoolean", typed, text, resultSet -> resultSet.getBoolean(index));
                    assertSame(description + " getBytes", typed, text, resultSet -> resultSet.getBytes(index));
                    assertSame(description + " getDate", typed, text, resultSet -> resultSet.getDate(index));
                    assertSame(description + " getDate calendar", typed, text, resultSet -> resultSet.getDate(index, calendar));
                    assertSame(description + " getTime", typed, text, resultSet -> resultSet.getTime(index));
                    assertSame(description + " getTimestamp", typed, text, resultSet -> resultSet.getTimestamp(index));
                    assertSame(description + " getTimestamp calendar", typed, text, resultSet -> resultSet.getTimestamp(index, calendar));
                    assertSame(description + " getAsciiStream", typed, text, resultSet -> resultSet.getAsciiStream(index));
                    assertSame(description + " getArray", typed, text, resultSet -> resultSet.getArray(index));
                }
            }
            assertThat(typed.next()).isFalse();
            assertThat(text.next()).isFalse();
        }
    }

    private static void assertSame(String description, ResultSet typed, ResultSet text, Getter getter)
    {
        Object expected = read(text, getter);
        Object actual = read(typed, getter);
        assertThat(actual).as(description).isEqualTo(expected);
        assertThat(read(typed, ResultSet::wasNull)).as(description + " wasNull").isEqualTo(read(text, ResultSet::wasNull));
    }

    /**
     * The value a getter returns, or the class and message of the exception it throws, in a form that
     * compares by value.
     */
    private static Object read(ResultSet resultSet, Getter getter)
    {
        try {
            Object value = getter.get(resultSet);
            if (value instanceof byte[]) {
                return Bytes.asList((byte[]) value);
            }
            if (value instanceof InputStream) {
                return Bytes.asList(((InputStream) value).readAllBytes());
            }
            return value;
        }
        catch (SQLException | RuntimeException | IOException e) {
            List<String> chain = new ArrayList<>();
            for (Throwable throwable = e; throwable != null; throwable = throwable.getCause()) {
                chain.add(throwable.getClass().getName() + ": " + throwable.getMessage());
            }
            return chain;
        }
    }

    private static DecimalValue decimal(Random random, int precision, int scale)
    {
        BigInteger limit = BigInteger.TEN.pow(precision);
        BigInteger unscaled = new BigInteger(random.nextInt(limit.bitLength() + 1), random).mod(limit);
        if (random.nextBoolean()) {
            unscaled = unscaled.negate();
        }
        if (random.nextInt(20) == 0) {
            unscaled = random.nextBoolean() ? limit.subtract(BigInteger.ONE) : BigInteger.ONE.subtract(limit);
        }
        return new DecimalValue(unscaled.shiftRight(Long.SIZE).longValue(), unscaled.longValue(), scale);
    }

    private static LocalDate date(Random random)
    {
        if (random.nextInt(20) == 0) {
            // outside the years the date formatter reads, and around the Julian to Gregorian change
            List<LocalDate> edges = ImmutableList.of(LocalDate.of(0, 1, 1), LocalDate.of(-1, 12, 31), LocalDate.of(1, 1, 1), LocalDate.of(1582, 10, 4), LocalDate.of(1582, 10, 15), LocalDate.of(9999, 12, 31), LocalDate.of(10000, 1, 1));
            return edges.get(random.nextInt(edges.size()));
        }
        return LocalDate.ofEpochDay(random.nextInt(4_000_000) - 1_000_000);
    }

    private static TimestampValue timestamp(Random random, int precision, Optional<String> zoneId)
    {
        long epochSecond = random.nextLong() % 200_000_000_000L;
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
        long picos = Math.floorMod(random.nextLong(), 1_000_000_000_000L);
        long unit = BigInteger.TEN.pow(12 - precision).longValueExact();
        return new TimestampValue(
                dateTime.getYear(),
                dateTime.getMonthValue(),
                dateTime.getDayOfMonth(),
                dateTime.getHour(),
                dateTime.getMinute(),
                dateTime.getSecond(),
                picos - picos % unit,
                precision,
                zoneId);
    }

    private static Column column(String type, int... arguments)
    {
        ImmutableList.Builder<ClientTypeSignatureParameter> parameters = ImmutableList.builder();
        for (int argument : arguments) {
            parameters.add(ClientTypeSignatureParameter.ofLong(argument));
        }
        ClientTypeSignature signature = new ClientTypeSignature(type, parameters.build());
        return new Column("col", signature.toString(), signature);
    }

    private interface ValueGenerator
    {
        Object next();
    }

    private interface Getter
    {
        Object get(ResultSet resultSet)
                throws SQLException;
    }
}
