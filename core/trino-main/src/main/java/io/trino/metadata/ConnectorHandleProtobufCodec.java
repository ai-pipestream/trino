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

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.server.protocol.v1.ConnectorHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;

import java.util.Optional;

import static io.trino.server.protobuf.ProtoIdentifiers.parseCatalogHandle;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Encodes connector handles as the engine {@code ConnectorHandle} protobuf
 * message: the owning catalog handle id plus the connector's own protobuf
 * payload. The codec comes from the catalog's
 * {@link ConnectorHandleCodecProvider}, resolved through the node's
 * {@link ConnectorServicesProvider}, so the same lookup works on the
 * coordinator and on workers. A catalog whose connector provides no codec
 * for the handle family fails loudly: under protobuf control-plane encoding
 * there is no JSON fallback.
 */
public class ConnectorHandleProtobufCodec
{
    private final ConnectorServicesProvider connectorServicesProvider;

    @Inject
    public ConnectorHandleProtobufCodec(ConnectorServicesProvider connectorServicesProvider)
    {
        this.connectorServicesProvider = requireNonNull(connectorServicesProvider, "connectorServicesProvider is null");
    }

    public <T> ConnectorHandle encode(CatalogHandle catalogHandle, HandleFamily<T> family, T handle)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(family, "family is null");
        requireNonNull(handle, "handle is null");
        ConnectorHandleCodec<T> codec = resolveCodec(catalogHandle, family, handle.getClass());
        byte[] payload;
        try {
            payload = codec.serialize(handle);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                    "Protobuf handle codec of catalog '%s' failed to encode %s handle %s",
                    catalogHandle.getCatalogName(),
                    family,
                    handle.getClass().getName()), e);
        }
        if (payload == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                    "Protobuf handle codec of catalog '%s' returned null for %s handle %s",
                    catalogHandle.getCatalogName(),
                    family,
                    handle.getClass().getName()));
        }
        return ConnectorHandle.newBuilder()
                .setCatalogHandle(catalogHandle.getId())
                .setPayload(ByteString.copyFrom(payload))
                .build();
    }

    public <T> T decode(HandleFamily<T> family, ConnectorHandle proto)
    {
        requireNonNull(family, "family is null");
        requireNonNull(proto, "proto is null");
        CatalogHandle catalogHandle = parseCatalogHandle(proto.getCatalogHandle());
        ConnectorHandleCodec<T> codec = resolveCodec(catalogHandle, family, family.handleClass());
        T handle;
        try {
            handle = codec.deserialize(proto.getPayload().toByteArray());
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                    "Protobuf handle codec of catalog '%s' failed to decode a %s handle",
                    catalogHandle.getCatalogName(),
                    family), e);
        }
        if (handle == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                    "Protobuf handle codec of catalog '%s' decoded a %s handle as null",
                    catalogHandle.getCatalogName(),
                    family));
        }
        if (!family.handleClass().isInstance(handle)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format(
                    "Protobuf handle codec of catalog '%s' decoded a %s handle as %s, which is not a %s",
                    catalogHandle.getCatalogName(),
                    family,
                    handle.getClass().getName(),
                    family.handleClass().getName()));
        }
        return handle;
    }

    /**
     * The error raised when a connector ships no codec for a handle family
     * it uses. Shared with engine-owned codecs that embed another catalog's
     * handles, so every missing codec reports the same way.
     */
    public static TrinoException missingCodec(CatalogHandle catalogHandle, HandleFamily<?> family, Class<?> handleType)
    {
        return new TrinoException(NOT_SUPPORTED, format(
                "Connector for catalog '%s' does not provide a protobuf handle codec for %s handles of type %s. " +
                        "Protobuf control-plane encoding requires a ConnectorHandleCodecProvider; there is no JSON fallback.",
                catalogHandle.getCatalogName(),
                family,
                handleType.getName()));
    }

    private <T> ConnectorHandleCodec<T> resolveCodec(CatalogHandle catalogHandle, HandleFamily<T> family, Class<?> handleType)
    {
        ConnectorServices connectorServices;
        try {
            connectorServices = connectorServicesProvider.getConnectorServices(catalogHandle);
        }
        catch (RuntimeException e) {
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Catalog '%s' is not available on this node", catalogHandle.getCatalogName()), e);
        }
        Optional<ConnectorHandleCodecProvider> provider = connectorServices.getHandleCodecProvider();
        return provider.flatMap(family::codecFrom)
                .orElseThrow(() -> missingCodec(catalogHandle, family, handleType));
    }
}
