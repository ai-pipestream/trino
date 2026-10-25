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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeZoneKey;
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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.io.OutputStream.nullOutputStream;
import static java.lang.String.format;

/**
 * Encodes and decodes a table with each spooled query data encoding. {@code lineitem} is TPC-H
 * {@code lineitem} with the column types the TPC-H connector exposes; {@code types} is generated, with
 * decimals, timestamps, timestamps with time zone, full precision doubles, a uuid, an array and nulls.
 * One operation is the whole table, split into segments of {@link #ROWS_PER_SEGMENT} rows.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
public class BenchmarkQueryDataEncoding
{
    private static final int ROWS_PER_PAGE = 8192;
    // about 10 MB of json for both tables, below the default protocol.spooling.max-segment-size of 16 MB
    private static final int ROWS_PER_SEGMENT = 1 << 16;
    private static final int TYPES_ROWS = 2_000_000;
    private static final List<TimeZoneKey> TIME_ZONES = ImmutableList.of(
            UTC_KEY,
            getTimeZoneKey("America/New_York"),
            getTimeZoneKey("Europe/Warsaw"),
            getTimeZoneKey("Asia/Kolkata"),
            getTimeZoneKey("+05:45"));

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

    private static final DecimalType PRICE = createDecimalType(12, 2);
    private static final DecimalType AMOUNT = createDecimalType(38, 10);
    private static final List<OutputColumn> TYPES_COLUMNS = ImmutableList.of(
            new OutputColumn(0, "id", BIGINT),
            new OutputColumn(1, "price", PRICE),
            new OutputColumn(2, "amount", AMOUNT),
            new OutputColumn(3, "created", TIMESTAMP_MICROS),
            new OutputColumn(4, "updated", TIMESTAMP_TZ_MILLIS),
            new OutputColumn(5, "ratio", DOUBLE),
            new OutputColumn(6, "active", BOOLEAN),
            new OutputColumn(7, "name", VARCHAR),
            new OutputColumn(8, "score", INTEGER),
            new OutputColumn(9, "key", UUID),
            new OutputColumn(10, "tags", new ArrayType(VARCHAR)));

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"json", "json+zstd", "json+lz4", "protobuf", "protobuf+zstd", "protobuf+lz4"})
        private String encoding = "protobuf+zstd";

        @Param({"lineitem", "types"})
        private String table = "lineitem";

        @Param("1")
        private double scaleFactor = 1;

        private final List<List<Page>> segments = new ArrayList<>();
        private final List<byte[]> encodedSegments = new ArrayList<>();
        private final List<DataAttributes> segmentAttributes = new ArrayList<>();
        private QueryDataEncoder encoder;
        private QueryDataDecoder.Factory decoderFactory;
        private List<Column> clientColumns;
        private long rows;
        private long encodedBytes;

        /**
         * Generates and encodes {@code table}, for benchmarks of other modules that decode it.
         */
        public static BenchmarkData create(String table, String encoding)
                throws IOException
        {
            BenchmarkData data = new BenchmarkData();
            data.table = table;
            data.encoding = encoding;
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws IOException
        {
            List<OutputColumn> columns = switch (table) {
                case "lineitem" -> LINEITEM_COLUMNS;
                case "types" -> TYPES_COLUMNS;
                default -> throw new IllegalArgumentException("Unknown table: " + table);
            };
            List<Type> types = columns.stream().map(OutputColumn::type).collect(toImmutableList());
            PageBuilder pageBuilder = new PageBuilder(types);
            List<Page> segment = new ArrayList<>();
            int segmentRows = 0;
            Iterator<LineItem> lineItems = new LineItemGenerator(scaleFactor, 1, 1).iterator();
            Random random = new Random(42);
            while (table.equals("lineitem") ? lineItems.hasNext() : rows < TYPES_ROWS) {
                pageBuilder.declarePosition();
                if (table.equals("lineitem")) {
                    writeLineItem(pageBuilder, types, lineItems.next());
                }
                else {
                    writeTypesRow(pageBuilder, rows, random);
                }
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

        private static void writeLineItem(PageBuilder pageBuilder, List<Type> types, LineItem lineItem)
        {
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
        }

        private static void writeTypesRow(PageBuilder pageBuilder, long id, Random random)
        {
            // 2000-01-01 to 2030-01-01
            long epochMillis = 946_684_800_000L + (long) (random.nextDouble() * 946_684_800_000L);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), id);
            PRICE.writeLong(pageBuilder.getBlockBuilder(1), random.nextLong(100_000_000L));
            AMOUNT.writeObject(pageBuilder.getBlockBuilder(2), Int128.valueOf(new BigInteger(100, random).subtract(BigInteger.ONE.shiftLeft(99))));
            TIMESTAMP_MICROS.writeLong(pageBuilder.getBlockBuilder(3), epochMillis * 1000 + random.nextInt(1000));
            TIMESTAMP_TZ_MILLIS.writeLong(pageBuilder.getBlockBuilder(4), packDateTimeWithZone(epochMillis + random.nextInt(86_400_000), TIME_ZONES.get(random.nextInt(TIME_ZONES.size()))));
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(5), random.nextGaussian());
            BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(6), random.nextBoolean());
            if (random.nextInt(5) == 0) {
                pageBuilder.getBlockBuilder(7).appendNull();
            }
            else {
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(7), utf8Slice(randomText(random, 5 + random.nextInt(26))));
            }
            if (random.nextInt(10) < 3) {
                pageBuilder.getBlockBuilder(8).appendNull();
            }
            else {
                INTEGER.writeLong(pageBuilder.getBlockBuilder(8), random.nextInt(1_000_000));
            }
            UUID.writeSlice(pageBuilder.getBlockBuilder(9), javaUuidToTrinoUuid(new java.util.UUID(random.nextLong(), random.nextLong())));
            int tags = random.nextInt(5);
            ((ArrayBlockBuilder) pageBuilder.getBlockBuilder(10)).buildEntry(elements -> {
                for (int i = 0; i < tags; i++) {
                    VARCHAR.writeSlice(elements, utf8Slice("tag" + random.nextInt(100)));
                }
            });
        }

        private static String randomText(Random random, int length)
        {
            char[] text = new char[length];
            for (int i = 0; i < length; i++) {
                text[i] = (char) ('a' + random.nextInt(26));
            }
            return new String(text);
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

        public List<byte[]> getEncodedSegments()
        {
            return encodedSegments;
        }

        public List<DataAttributes> getSegmentAttributes()
        {
            return segmentAttributes;
        }

        public List<Column> getClientColumns()
        {
            return clientColumns;
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
        for (String table : List.of("lineitem", "types")) {
            for (String encoding : List.of("json", "json+zstd", "json+lz4", "protobuf", "protobuf+zstd", "protobuf+lz4")) {
                BenchmarkData data = BenchmarkData.create(table, encoding);
                System.out.println(format("%-9s %-14s rows=%d segments=%d bytes=%d", table, encoding, data.getRows(), data.getSegments(), data.getEncodedBytes()));
            }
        }

        benchmark(BenchmarkQueryDataEncoding.class)
                .withOptions(options -> options.jvmArgsAppend("-Xmx8g"))
                .run();
    }
}
