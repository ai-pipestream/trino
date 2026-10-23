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
package io.trino.protobuf.handles;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorHandleCodec;

import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A {@link ConnectorHandleCodec} backed by a protobuf message: the connector
 * supplies the mapping between its handle class and the generated message, and
 * this class owns the byte handling. A handle of another class than the one
 * declared, or bytes that do not parse, raise a categorized
 * {@link TrinoException} naming the handle class.
 *
 * @param <F> the SPI handle family, such as {@code ConnectorSplit}
 * @param <H> the connector's handle class within that family
 * @param <P> the generated protobuf message for the handle
 */
public final class ProtobufHandleCodec<F, H extends F, P extends Message>
        implements ConnectorHandleCodec<F>
{
    private final Class<H> handleClass;
    private final Parser<P> parser;
    private final Function<H, P> toProto;
    private final Function<P, H> fromProto;

    private ProtobufHandleCodec(Class<H> handleClass, Parser<P> parser, Function<H, P> toProto, Function<P, H> fromProto)
    {
        this.handleClass = requireNonNull(handleClass, "handleClass is null");
        this.parser = requireNonNull(parser, "parser is null");
        this.toProto = requireNonNull(toProto, "toProto is null");
        this.fromProto = requireNonNull(fromProto, "fromProto is null");
    }

    public static <F, H extends F, P extends Message> ConnectorHandleCodec<F> protobufHandleCodec(
            Class<H> handleClass,
            Parser<P> parser,
            Function<H, P> toProto,
            Function<P, H> fromProto)
    {
        return new ProtobufHandleCodec<>(handleClass, parser, toProto, fromProto);
    }

    @Override
    public byte[] serialize(F handle)
    {
        requireNonNull(handle, "handle is null");
        if (!handleClass.isInstance(handle)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf handle codec for %s cannot encode %s".formatted(handleClass.getName(), handle.getClass().getName()));
        }
        return toProto.apply(handleClass.cast(handle)).toByteArray();
    }

    @Override
    public F deserialize(byte[] bytes)
    {
        requireNonNull(bytes, "bytes is null");
        P proto;
        try {
            proto = parser.parseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf %s".formatted(handleClass.getSimpleName()), e);
        }
        try {
            return fromProto.apply(proto);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            // handle constructors reject inconsistent state with IllegalArgumentException, NullPointerException and the like
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf %s does not describe a valid handle: %s".formatted(handleClass.getSimpleName(), e.getMessage()), e);
        }
    }
}
