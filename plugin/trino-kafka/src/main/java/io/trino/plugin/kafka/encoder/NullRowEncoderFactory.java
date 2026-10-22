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
package io.trino.plugin.kafka.encoder;

import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Encodes the absent key or message of a table that defines no columns for it as a {@code null} Kafka key or value.
 */
public class NullRowEncoderFactory
        implements RowEncoderFactory
{
    private static final RowEncoder NULL_ROW_ENCODER = new RowEncoder()
    {
        @Override
        public void appendColumnValue(Block block, int position)
        {
            throw new IllegalStateException("No columns are defined");
        }

        @Override
        public byte[] toByteArray()
        {
            return null;
        }

        @Override
        public void close() {}
    };

    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        checkArgument(rowEncoderSpec.columnHandles().isEmpty(), "No columns are expected for the %s of topic '%s'", rowEncoderSpec.kafkaFieldType(), rowEncoderSpec.topic());
        return NULL_ROW_ENCODER;
    }
}
