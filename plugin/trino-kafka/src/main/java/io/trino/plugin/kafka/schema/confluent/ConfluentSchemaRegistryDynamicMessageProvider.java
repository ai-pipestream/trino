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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.spi.TrinoException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfluentSchemaRegistryDynamicMessageProvider
        implements DynamicMessageProvider
{
    private static final int MAGIC_BYTE = 0;
    private final SchemaRegistryClient schemaRegistryClient;
    private final NonEvictableLoadingCache<MessageType, Descriptor> descriptorCache;

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        descriptorCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::lookupDescriptor));
    }

    @Override
    public DynamicMessage parseDynamicMessage(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte magicByte = buffer.get();
        checkArgument(magicByte == MAGIC_BYTE, "Invalid MagicByte");
        int schemaId = buffer.getInt();
        List<Integer> messageIndex = MessageIndexes.readFrom(buffer).indexes();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        try {
            return DynamicMessage.parseFrom(
                    getDescriptor(new MessageType(schemaId, messageIndex)),
                    byteArrayInputStream);
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.", e);
        }
    }

    private Descriptor getDescriptor(MessageType messageType)
    {
        try {
            return descriptorCache.getUnchecked(messageType);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private Descriptor lookupDescriptor(MessageType messageType)
    {
        ParsedSchema schema;
        try {
            schema = schemaRegistryClient.getSchemaById(messageType.schemaId());
        }
        catch (IOException | RestClientException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Looking up schemaId '%s' from confluent schema registry failed", messageType.schemaId()), e);
        }
        checkArgument(schema instanceof ProtobufSchema, "schema should be an instance of ProtobufSchema");
        ProtobufSchema protobufSchema = (ProtobufSchema) schema;

        // Resolve the message the same way the Confluent deserializer does
        String messageName;
        try {
            messageName = protobufSchema.toMessageName(new MessageIndexes(messageType.messageIndex()));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, format("Message index %s does not exist in schema with id %s", messageType.messageIndex(), messageType.schemaId()), e);
        }
        return protobufSchema.toDescriptor(messageName);
    }

    private record MessageType(int schemaId, List<Integer> messageIndex)
    {
        MessageType
        {
            // An empty message index refers to the first message
            messageIndex = messageIndex.isEmpty() ? ImmutableList.of(0) : ImmutableList.copyOf(messageIndex);
        }
    }

    public static class Factory
            implements DynamicMessageProvider.Factory
    {
        private final SchemaRegistryClient schemaRegistryClient;

        @Inject
        public Factory(SchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        }

        @Override
        public DynamicMessageProvider create(Optional<String> protoFile)
        {
            return new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient);
        }
    }
}
