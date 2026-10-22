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
package io.trino.client.spooling.encoding;

import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.protobuf.ProtobufRowIterator;
import io.trino.client.spooling.encoding.protobuf.ProtobufValueDecoders.ValueDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static io.trino.client.spooling.encoding.protobuf.ProtobufValueDecoders.createValueDecoders;
import static java.util.Objects.requireNonNull;

/**
 * Decodes segments of the {@code protobuf} encoding, defined in {@code trino_spooled_segment.proto}.
 */
public class ProtobufQueryDataDecoder
        implements QueryDataDecoder
{
    private static final String ENCODING = "protobuf";

    private final ValueDecoder[] decoders;

    ProtobufQueryDataDecoder(ValueDecoder[] decoders)
    {
        this.decoders = requireNonNull(decoders, "decoders is null");
    }

    @Override
    public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException
    {
        return new ProtobufRowIterator(input, decoders);
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes, boolean supportsVariantBinary)
        {
            return new ProtobufQueryDataDecoder(createValueDecoders(columns, supportsVariantBinary));
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes, boolean supportsVariantBinary)
        {
            return new ZstdQueryDataDecoder(super.create(columns, queryAttributes, supportsVariantBinary));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    public static class Lz4Factory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes, boolean supportsVariantBinary)
        {
            return new Lz4QueryDataDecoder(super.create(columns, queryAttributes, supportsVariantBinary));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+lz4";
        }
    }
}
