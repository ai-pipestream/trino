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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs spooled queries through the JDBC driver with each query data encoding and
 * checks that every encoding yields the values the {@code json} encoding yields.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestJdbcSpooledEncodings
{
    private static final List<String> PROTOBUF_ENCODINGS = ImmutableList.of("protobuf", "protobuf+zstd", "protobuf+lz4");

    // One column per Trino type family, including nulls and nested types
    private static final String TYPES_QUERY = "SELECT " + String.join(", ", ImmutableList.of(
            "true",
            "TINYINT '-128'",
            "SMALLINT '32767'",
            "INTEGER '-2147483648'",
            "BIGINT '9223372036854775807'",
            "REAL '1.5'",
            "DOUBLE '-2.25'",
            "DECIMAL '12345.678'",
            "DECIMAL '-12345678901234567890123456789.123456789'",
            "'zażółć gęślą jaźń'",
            "CAST('ab' AS CHAR(5))",
            "X'0001FF'",
            "DATE '2024-02-29'",
            "TIME '12:34:56.123456789012'",
            "TIME '12:34:56.123 +05:30'",
            "TIMESTAMP '2024-02-29 12:34:56.123456789012'",
            "TIMESTAMP '2024-02-29 12:34:56.123 Europe/Warsaw'",
            "INTERVAL '1-2' YEAR TO MONTH",
            "INTERVAL '1 02:03:04.567' DAY TO SECOND",
            "JSON '{\"a\": [1, 2, null]}'",
            "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
            "IPADDRESS '2001:db8::1'",
            "ARRAY[1, NULL, 3]",
            "ARRAY[ARRAY['a', NULL], NULL]",
            "MAP(ARRAY['a', 'b'], ARRAY[1.5, NULL])",
            "CAST(ROW(1, 'x', ARRAY[true]) AS ROW(a BIGINT, b VARCHAR, c ARRAY(BOOLEAN)))",
            "CAST(NULL AS BIGINT)",
            "CAST(NULL AS ARRAY(BIGINT))"));

    private DistributedQueryRunner queryRunner;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        // The query runner's own client uses json; the connections below pick the encoding through the JDBC URL
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema("tiny")
                        .build())
                .withProtocolSpooling("json")
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testAllTypesMatchJsonEncoding()
            throws Exception
    {
        QueryResult expected = execute("json", TYPES_QUERY);
        assertThat(expected.encoding).hasValue("json");
        assertThat(expected.rows).hasSize(1);

        for (String encoding : PROTOBUF_ENCODINGS) {
            QueryResult actual = execute(encoding, TYPES_QUERY);
            assertThat(actual.encoding).hasValue(encoding);
            assertThat(actual.rows).isEqualTo(expected.rows);
        }
    }

    @Test
    public void testLargeResultSpansSegments()
            throws Exception
    {
        String query = "SELECT * FROM lineitem ORDER BY orderkey, linenumber";
        QueryResult expected = execute("json", query);
        assertThat(expected.rows).hasSize(60175);

        for (String encoding : PROTOBUF_ENCODINGS) {
            QueryResult actual = execute(encoding, query);
            assertThat(actual.encoding).hasValue(encoding);
            assertThat(actual.rows).isEqualTo(expected.rows);
        }
    }

    @Test
    public void testServerPicksFirstSupportedEncoding()
            throws Exception
    {
        assertThat(execute("protobuf+zstd,json+zstd", "SELECT 1").encoding).hasValue("protobuf+zstd");
        assertThat(execute("json+zstd,protobuf+zstd", "SELECT 1").encoding).hasValue("json+zstd");
        assertThat(execute("protobuf+lz4,protobuf", "SELECT 1").encoding).hasValue("protobuf+lz4");
    }

    private QueryResult execute(String encoding, String sql)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/tpch/tiny?encoding=%s", queryRunner.getCoordinator().getAddress(), encoding);
        try (Connection connection = DriverManager.getConnection(url, "test", null);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<List<Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>(metaData.getColumnCount());
                for (int column = 1; column <= metaData.getColumnCount(); column++) {
                    row.add(comparable(resultSet.getObject(column)));
                }
                rows.add(row);
            }
            QueryId queryId = new QueryId(resultSet.unwrap(TrinoResultSet.class).getQueryId());
            Optional<String> queryDataEncoding = queryRunner.getCoordinator()
                    .getQueryManager()
                    .getFullQueryInfo(queryId)
                    .getSession()
                    .getQueryDataEncoding();
            return new QueryResult(rows, queryDataEncoding);
        }
    }

    // Arrays and binary values do not have a value based equals
    private static Object comparable(Object value)
            throws SQLException
    {
        if (value instanceof Array) {
            return comparable(Arrays.asList((Object[]) ((Array) value).getArray()));
        }
        if (value instanceof byte[]) {
            return BaseEncoding.base16().encode((byte[]) value);
        }
        if (value instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object element : (List<?>) value) {
                result.add(comparable(element));
            }
            return result;
        }
        if (value instanceof Map) {
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                result.put(comparable(entry.getKey()), comparable(entry.getValue()));
            }
            return result;
        }
        return value;
    }

    private static class QueryResult
    {
        private final List<List<Object>> rows;
        private final Optional<String> encoding;

        private QueryResult(List<List<Object>> rows, Optional<String> encoding)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.encoding = requireNonNull(encoding, "encoding is null");
        }
    }
}
