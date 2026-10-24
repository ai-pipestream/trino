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
package io.trino.server.protocol.spooling.encoding;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncodingConfig;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.io.OutputStream.nullOutputStream;
import static java.lang.String.format;

/**
 * Encodes and decodes whole tables with each spooled query data encoding: TPC-H
 * {@code lineitem} with the column types the TPC-H connector exposes, and a synthetic
 * type-rich table (decimals, timestamps with time zone, nulls, arrays, maps, uuids).
 * One operation is the whole table, split into segments of {@link #ROWS_PER_SEGMENT} rows.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class BenchmarkQueryDataEncoding
{
    private static final int ROWS_PER_PAGE = 8192;
    private static final int ROWS_PER_SEGMENT = 1 << 17;

    private static final List<OutputColumn> LINEITEM_COLUMNS = ImmutableList.of(
            new OutputColumn(0, "orderkey", BIGINT),
            new OutputColumn(1, "partkey", BIGINT),
            new OutputColumn(2, "suppkey", BIGINT),
            new OutputColumn(3, "linenumber", INTEGER),
            new OutputColumn(4, "quantity", DOUBLE),
            new OutputColumn(5, "extendedprice", DOUBLE),
            new OutputColumn(6, "discount", DOUBLE),
            new OutputColumn(7, "tax", DOUBLE),
            new OutputColumn(8, "returnflag", createVarcharType(1)),
            new OutputColumn(9, "linestatus", createVarcharType(1)),
            new OutputColumn(10, "shipdate", DATE),
            new OutputColumn(11, "commitdate", DATE),
            new OutputColumn(12, "receiptdate", DATE),
            new OutputColumn(13, "shipinstruct", createVarcharType(25)),
            new OutputColumn(14, "shipmode", createVarcharType(10)),
            new OutputColumn(15, "comment", createVarcharType(44)));

    private static final DecimalType RICH_AMOUNT = createDecimalType(19, 4);
    private static final DecimalType RICH_PRICE = createDecimalType(10, 2);
    private static final TimestampType RICH_CREATED = createTimestampType(6);
    private static final TimestampWithTimeZoneType RICH_CREATED_TZ = createTimestampWithTimeZoneType(3);
    private static final int RICH_ROWS = 1 << 20;
    private static final List<TimeZoneKey> RICH_ZONES = ImmutableList.of(
            getTimeZoneKey("UTC"),
            getTimeZoneKey("Europe/Warsaw"),
            getTimeZoneKey("America/New_York"),
            getTimeZoneKey("America/Los_Angeles"),
            getTimeZoneKey("Asia/Kathmandu"));

    // A type-rich table lineitem does not exercise: long and short decimals, timestamps with and
    // without a time zone, nulls, longer strings, arrays, maps and uuids, generated deterministically
    private static final List<OutputColumn> RICH_COLUMNS = ImmutableList.of(
            new OutputColumn(0, "id", BIGINT),
            new OutputColumn(1, "amount", RICH_AMOUNT),
            new OutputColumn(2, "price", RICH_PRICE),
            new OutputColumn(3, "created", RICH_CREATED),
            new OutputColumn(4, "created_tz", RICH_CREATED_TZ),
            new OutputColumn(5, "score", DOUBLE),
            new OutputColumn(6, "customer", createVarcharType(100)),
            new OutputColumn(7, "active", BOOLEAN),
            new OutputColumn(8, "day", DATE),
            new OutputColumn(9, "nums", new ArrayType(BIGINT)),
            new OutputColumn(10, "props", mapTypeForBenchmark()),
            new OutputColumn(11, "uid", UUID));

    private static MapType mapTypeForBenchmark()
    {
        return io.trino.util.StructuralTestUtil.mapType(createVarcharType(20), BIGINT);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"json", "json+zstd", "json+lz4", "protobuf", "protobuf+zstd", "protobuf+lz4"})
        private String encoding = "protobuf+zstd";

        @Param({"lineitem", "rich"})
        private String table = "lineitem";

        @Param("1")
        private double scaleFactor = 1;

        private final List<List<Page>> segments = new ArrayList<>();
        private final List<byte[]> encodedSegments = new ArrayList<>();
        private final List<DataAttributes> segmentAttributes = new ArrayList<>();
        private QueryDataEncoder encoder;
        private QueryDataDecoder.Factory decoderFactory;
        private List<OutputColumn> columns;
        private List<Column> clientColumns;
        private long rows;
        private long encodedBytes;

        @Setup
        public void setup()
                throws IOException
        {
            columns = table.equals("rich") ? RICH_COLUMNS : LINEITEM_COLUMNS;
            List<Type> types = columns.stream().map(OutputColumn::type).collect(toImmutableList());
            if (table.equals("rich")) {
                buildRichSegments(types);
            }
            else {
                PageBuilder pageBuilder = new PageBuilder(types);
                List<Page> segment = new ArrayList<>();
                int segmentRows = 0;
                Iterator<LineItem> lineItems = new LineItemGenerator(scaleFactor, 1, 1).iterator();
                while (lineItems.hasNext()) {
                    LineItem lineItem = lineItems.next();
                    pageBuilder.declarePosition();
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.orderKey());
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(1), lineItem.partKey());
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(2), lineItem.supplierKey());
                    INTEGER.writeLong(pageBuilder.getBlockBuilder(3), lineItem.lineNumber());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), lineItem.quantity());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(5), lineItem.extendedPrice());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(6), lineItem.discount());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(7), lineItem.tax());
                    types.get(8).writeSlice(pageBuilder.getBlockBuilder(8), utf8Slice(lineItem.returnFlag()));
                    types.get(9).writeSlice(pageBuilder.getBlockBuilder(9), utf8Slice(lineItem.status()));
                    DATE.writeLong(pageBuilder.getBlockBuilder(10), lineItem.shipDate());
                    DATE.writeLong(pageBuilder.getBlockBuilder(11), lineItem.commitDate());
                    DATE.writeLong(pageBuilder.getBlockBuilder(12), lineItem.receiptDate());
                    types.get(13).writeSlice(pageBuilder.getBlockBuilder(13), utf8Slice(lineItem.shipInstructions()));
                    types.get(14).writeSlice(pageBuilder.getBlockBuilder(14), utf8Slice(lineItem.shipMode()));
                    types.get(15).writeSlice(pageBuilder.getBlockBuilder(15), utf8Slice(lineItem.comment()));
                    rows++;
                    segmentRows++;
                    if (pageBuilder.getPositionCount() == ROWS_PER_PAGE) {
                        segment.add(pageBuilder.build());
                        pageBuilder.reset();
                    }
                    if (segmentRows == ROWS_PER_SEGMENT) {
                        if (!pageBuilder.isEmpty()) {
                            segment.add(pageBuilder.build());
                            pageBuilder.reset();
                        }
                        segments.add(ImmutableList.copyOf(segment));
                        segment.clear();
                        segmentRows = 0;
                    }
                }
                if (!pageBuilder.isEmpty()) {
                    segment.add(pageBuilder.build());
                }
                if (!segment.isEmpty()) {
                    segments.add(ImmutableList.copyOf(segment));
                }
            }
            encoder = createEncoderFactory(encoding).create(TEST_SESSION, columns);
            decoderFactory = QueryDataDecoders.get(encoding);
            clientColumns = columns.stream()
                    .map(column -> createColumn(column.columnName(), column.type(), true, true, true, true, true))
                    .collect(toImmutableList());

            for (List<Page> pages : segments) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataAttributes attributes = encoder.encodeTo(output, pages);
                encodedSegments.add(output.toByteArray());
                segmentAttributes.add(attributes);
                encodedBytes += output.size();
            }
        }

        private void buildRichSegments(List<Type> types)
        {
            PageBuilder pageBuilder = new PageBuilder(types);
            List<Page> segment = new ArrayList<>();
            int segmentRows = 0;
            for (int i = 0; i < RICH_ROWS; i++) {
                int row = i;
                pageBuilder.declarePosition();
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), row);
                RICH_AMOUNT.writeObject(pageBuilder.getBlockBuilder(1), Int128.valueOf(row * 1_234_567L + 89));
                RICH_PRICE.writeLong(pageBuilder.getBlockBuilder(2), (row % 100_000) * 101L + 7);
                RICH_CREATED.writeLong(pageBuilder.getBlockBuilder(3), 1_600_000_000_000_000L + row * 999_983L);
                RICH_CREATED_TZ.writeLong(pageBuilder.getBlockBuilder(4), packDateTimeWithZone(1_600_000_000_000L + row * 61_223L, RICH_ZONES.get(row % RICH_ZONES.size())));
                writeNullable(pageBuilder.getBlockBuilder(5), row % 7 == 0, builder -> DOUBLE.writeDouble(builder, (row % 10_000) / 37.0));
                types.get(6).writeSlice(pageBuilder.getBlockBuilder(6), utf8Slice("customer-" + (row % 100_000) + "-" + "x".repeat(row % 32)));
                writeNullable(pageBuilder.getBlockBuilder(7), row % 11 == 0, builder -> BOOLEAN.writeBoolean(builder, row % 3 != 0));
                DATE.writeLong(pageBuilder.getBlockBuilder(8), 18_000 + (row % 2_000));
                ((ArrayBlockBuilder) pageBuilder.getBlockBuilder(9)).buildEntry(elements -> {
                    for (int j = 0; j < row % 6; j++) {
                        BIGINT.writeLong(elements, row * 10L + j);
                    }
                });
                ((MapBlockBuilder) pageBuilder.getBlockBuilder(10)).buildEntry((keys, values) -> {
                    for (int j = 0; j < row % 4; j++) {
                        types.get(10).getTypeParameters().getFirst().writeSlice(keys, utf8Slice("k" + j));
                        BIGINT.writeLong(values, row * 10L + j);
                    }
                });
                UUID.writeSlice(pageBuilder.getBlockBuilder(11), javaUuidToTrinoUuid(new UUID(row * 0x9E3779B97F4A7C15L, row ^ 0xC2B2AE3D27D4EB4FL)));
                rows++;
                segmentRows++;
                if (pageBuilder.getPositionCount() == ROWS_PER_PAGE) {
                    segment.add(pageBuilder.build());
                    pageBuilder.reset();
                }
                if (segmentRows == ROWS_PER_SEGMENT) {
                    if (!pageBuilder.isEmpty()) {
                        segment.add(pageBuilder.build());
                        pageBuilder.reset();
                    }
                    segments.add(ImmutableList.copyOf(segment));
                    segment.clear();
                    segmentRows = 0;
                }
            }
            if (!pageBuilder.isEmpty()) {
                segment.add(pageBuilder.build());
            }
            if (!segment.isEmpty()) {
                segments.add(ImmutableList.copyOf(segment));
            }
        }

        private static void writeNullable(BlockBuilder builder, boolean isNull, java.util.function.Consumer<BlockBuilder> writer)
        {
            if (isNull) {
                builder.appendNull();
            }
            else {
                writer.accept(builder);
            }
        }

        public long getRows()
        {
            return rows;
        }

        public long getEncodedBytes()
        {
            return encodedBytes;
        }

        public int getSegments()
        {
            return segments.size();
        }
    }

    @Benchmark
    public long encode(BenchmarkData data)
            throws IOException
    {
        long bytes = 0;
        for (List<Page> pages : data.segments) {
            CountingOutputStream output = new CountingOutputStream(nullOutputStream());
            data.encoder.encodeTo(output, pages);
            bytes += output.getCount();
        }
        return bytes;
    }

    @Benchmark
    public long decode(BenchmarkData data)
            throws IOException
    {
        long rows = 0;
        for (int i = 0; i < data.encodedSegments.size(); i++) {
            QueryDataDecoder decoder = data.decoderFactory.create(data.clientColumns, DataAttributes.empty(), true);
            try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(data.encodedSegments.get(i)), data.segmentAttributes.get(i))) {
                while (iterator.hasNext()) {
                    iterator.next();
                    rows++;
                }
            }
        }
        return rows;
    }

    private static QueryDataEncoder.Factory createEncoderFactory(String encoding)
    {
        QueryDataEncodingConfig config = new QueryDataEncodingConfig();
        JsonMapper mapper = new JsonMapper();
        return switch (encoding) {
            case "json" -> new JsonQueryDataEncoder.Factory(mapper);
            case "json+zstd" -> new JsonQueryDataEncoder.ZstdFactory(config, mapper);
            case "json+lz4" -> new JsonQueryDataEncoder.Lz4Factory(config, mapper);
            case "protobuf" -> new ProtobufQueryDataEncoder.Factory(mapper);
            case "protobuf+zstd" -> new ProtobufQueryDataEncoder.ZstdFactory(config, mapper);
            case "protobuf+lz4" -> new ProtobufQueryDataEncoder.Lz4Factory(config, mapper);
            default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
        };
    }

    static void main()
            throws Exception
    {
        // print the encoded size of each encoding before measuring the times
        for (String table : List.of("lineitem", "rich")) {
            for (String encoding : List.of("json", "json+zstd", "json+lz4", "protobuf", "protobuf+zstd", "protobuf+lz4")) {
                BenchmarkData data = new BenchmarkData();
                data.table = table;
                data.encoding = encoding;
                data.setup();
                System.out.println(format("%-8s %-14s rows=%d segments=%d bytes=%d", table, encoding, data.getRows(), data.getSegments(), data.getEncodedBytes()));
            }
        }

        benchmark(BenchmarkQueryDataEncoding.class)
                .withOptions(options -> options.jvmArgsAppend("-Xmx8g"))
                .run();
    }
}
