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

import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import io.trino.server.protocol.spooling.encoding.BenchmarkQueryDataEncoding.BenchmarkData;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.Objects.requireNonNull;

/**
 * Decodes the tables of {@link io.trino.server.protocol.spooling.encoding.BenchmarkQueryDataEncoding}
 * and reads every value through a JDBC {@link ResultSet}, which is what an application pays for a
 * result: {@code object} reads each value with {@link ResultSet#getObject(int)}, as tools that map
 * rows generically do, and {@code string} with {@link ResultSet#getString(int)}.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
public class BenchmarkResultSetDecoding
{
    @Param({"json", "json+zstd", "protobuf", "protobuf+zstd"})
    private String encoding = "protobuf";

    @Param({"lineitem", "types"})
    private String table = "types";

    @Param({"object", "string"})
    private String access = "object";

    private BenchmarkData data;
    private QueryDataDecoder.Factory decoderFactory;

    @Setup
    public void setup()
            throws IOException
    {
        data = BenchmarkData.create(table, encoding);
        decoderFactory = QueryDataDecoders.get(encoding);
    }

    @Benchmark
    public long readResultSet(Blackhole blackhole)
            throws IOException, SQLException
    {
        List<Column> columns = data.getClientColumns();
        boolean readObjects = access.equals("object");
        long rows = 0;
        for (int i = 0; i < data.getEncodedSegments().size(); i++) {
            QueryDataDecoder decoder = decoderFactory.create(columns, DataAttributes.empty(), true);
            CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(data.getEncodedSegments().get(i)), data.getSegmentAttributes().get(i));
            try (ResultSet resultSet = new IteratorResultSet(columns, iterator)) {
                while (resultSet.next()) {
                    for (int column = 1; column <= columns.size(); column++) {
                        blackhole.consume(readObjects ? resultSet.getObject(column) : resultSet.getString(column));
                    }
                    rows++;
                }
            }
        }
        return rows;
    }

    private static class IteratorResultSet
            extends AbstractTrinoResultSet
    {
        private boolean closed;

        IteratorResultSet(List<Column> columns, CloseableIterator<List<Object>> results)
        {
            super(Optional.empty(), columns, requireNonNull(results, "results is null"));
        }

        @Override
        public void close()
                throws SQLException
        {
            closed = true;
            // closes the iterator
            super.close();
        }

        @Override
        public boolean isClosed()
        {
            return closed;
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkResultSetDecoding.class)
                .withOptions(options -> options.jvmArgsAppend("-Xmx8g"))
                .run();
    }
}
