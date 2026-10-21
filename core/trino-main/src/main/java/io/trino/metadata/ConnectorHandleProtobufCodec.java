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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.trino.connector.CatalogHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Wraps connector handle bytes in the engine {@code ConnectorHandle}
 * protobuf message (owning catalog handle id plus the connector's own
 * protobuf payload) and resolves the codec from the catalog's
 * {@link ConnectorHandleCodecProvider}. A catalog whose connector provides
 * no codec for the handle family fails loudly: under protobuf control-plane
 * encoding there is no JSON fallback.
 */
public class ConnectorHandleProtobufCodec
{
    private final Function<CatalogHandle, Optional<ConnectorHandleCodecProvider>> codecProviders;

    public ConnectorHandleProtobufCodec(Function<CatalogHandle, Optional<ConnectorHandleCodecProvider>> codecProviders)
    {
        this.codecProviders = requireNonNull(codecProviders, "codecProviders is null");
    }

    public <T> byte[] encode(
            CatalogHandle catalogHandle,
            T handle,
            String handleFamily,
            Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> codecLookup)
    {
        ConnectorHandleCodec<T> codec = resolveCodec(catalogHandle, handle.getClass(), handleFamily, codecLookup);
        return io.trino.server.protocol.v1.ConnectorHandle.newBuilder()
                .setCatalogHandle(catalogHandle.getId())
                .setPayload(ByteString.copyFrom(codec.serialize(handle)))
                .build()
                .toByteArray();
    }

    public <T> T decode(
            byte[] bytes,
            Class<T> handleTypeForError,
            String handleFamily,
            Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> codecLookup)
    {
        io.trino.server.protocol.v1.ConnectorHandle proto;
        try {
            proto = io.trino.server.protocol.v1.ConnectorHandle.parseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf connector handle", e);
        }
        CatalogHandle catalogHandle = CatalogHandle.fromId(proto.getCatalogHandle());
        ConnectorHandleCodec<T> codec = resolveCodec(catalogHandle, handleTypeForError, handleFamily, codecLookup);
        return codec.deserialize(proto.getPayload().toByteArray());
    }

    private <T> ConnectorHandleCodec<T> resolveCodec(
            CatalogHandle catalogHandle,
            Class<?> handleType,
            String handleFamily,
            Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> codecLookup)
    {
        return codecProviders.apply(catalogHandle)
                .flatMap(codecLookup)
                .orElseThrow(() -> new TrinoException(
                        NOT_SUPPORTED,
                        format(
                                "Connector for catalog '%s' does not provide a protobuf handle codec for %s handles of type %s. " +
                                        "Protobuf control-plane encoding requires a ConnectorHandleCodecProvider; there is no JSON fallback.",
                                catalogHandle.getCatalogName(),
                                handleFamily,
                                handleType.getName())));
    }
}
