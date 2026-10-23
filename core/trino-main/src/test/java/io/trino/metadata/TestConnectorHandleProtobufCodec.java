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
package io.trino.metadata;

import io.trino.connector.CatalogHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorSplit;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConnectorHandleProtobufCodec
{
    private static final CatalogHandle CATALOG_HANDLE = CatalogHandle.createRootCatalogHandle(new CatalogName("test_catalog"), new CatalogVersion("test_version"));

    private record TestSplit(int value)
            implements ConnectorSplit {}

    private static final ConnectorHandleCodec<ConnectorSplit> TEST_SPLIT_CODEC = new ConnectorHandleCodec<>()
    {
        @Override
        public byte[] serialize(ConnectorSplit handle)
        {
            return Integer.toString(((TestSplit) handle).value()).getBytes(UTF_8);
        }

        @Override
        public ConnectorSplit deserialize(byte[] bytes)
        {
            return new TestSplit(Integer.parseInt(new String(bytes, UTF_8)));
        }
    };

    private static final ConnectorHandleCodecProvider PROVIDER_WITH_SPLIT_CODEC = new ConnectorHandleCodecProvider()
    {
        @Override
        public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
        {
            return Optional.of(TEST_SPLIT_CODEC);
        }
    };

    @Test
    public void testRoundTrip()
    {
        ConnectorHandleProtobufCodec codec = new ConnectorHandleProtobufCodec(_ -> Optional.of(PROVIDER_WITH_SPLIT_CODEC));
        byte[] encoded = codec.encode(CATALOG_HANDLE, new TestSplit(42), "split", ConnectorHandleCodecProvider::getSplitCodec);
        ConnectorSplit decoded = codec.decode(encoded, ConnectorSplit.class, "split", ConnectorHandleCodecProvider::getSplitCodec);
        assertThat(decoded).isEqualTo(new TestSplit(42));
    }

    @Test
    public void testConnectorWithoutProviderFailsLoudly()
    {
        ConnectorHandleProtobufCodec codec = new ConnectorHandleProtobufCodec(_ -> Optional.empty());
        assertThatThrownBy(() -> codec.encode(CATALOG_HANDLE, new TestSplit(1), "split", ConnectorHandleCodecProvider::getSplitCodec))
                .isInstanceOf(TrinoException.class)
                .matches(error -> ((TrinoException) error).getErrorCode() == NOT_SUPPORTED.toErrorCode(), "error code is NOT_SUPPORTED")
                .hasMessageContaining("test_catalog")
                .hasMessageContaining("split")
                .hasMessageContaining(TestSplit.class.getName());
    }

    @Test
    public void testProviderWithoutCodecForFamilyFailsLoudly()
    {
        ConnectorHandleProtobufCodec codec = new ConnectorHandleProtobufCodec(_ -> Optional.of(new ConnectorHandleCodecProvider() {}));
        assertThatThrownBy(() -> codec.encode(CATALOG_HANDLE, new TestSplit(1), "split", ConnectorHandleCodecProvider::getSplitCodec))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not provide a protobuf handle codec");
    }
}
