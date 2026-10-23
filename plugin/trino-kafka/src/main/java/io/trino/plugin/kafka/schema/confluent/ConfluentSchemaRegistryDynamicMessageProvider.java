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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.Frame;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * Decodes records in the Confluent wire format with the schema they were written with: the
 * schema id and message indexes in the frame select the message type, which is fetched from
 * the schema registry and cached. Ids are immutable in the registry, so the cache has no TTL.
 * <p>
 * The reader schema, which the table columns were derived from, is the first message of the
 * schema whose id the data schema parameter names; see {@link #readerSchemaReference(int)}.
 */
public class ConfluentSchemaRegistryDynamicMessageProvider
        implements DynamicMessageProvider
{
    private static final String READER_SCHEMA_REFERENCE_PREFIX = "schema-registry-id:";

    private final SchemaRegistryClient schemaRegistryClient;
    private final Optional<Integer> readerSchemaId;
    private final NonEvictableLoadingCache<MessageKey, Descriptor> descriptorCache;

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this(schemaRegistryClient, Optional.empty());
    }

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient, Optional<Integer> readerSchemaId)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.readerSchemaId = requireNonNull(readerSchemaId, "readerSchemaId is null");
        descriptorCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::lookupDescriptor));
    }

    /**
     * The data schema of a table whose columns were derived from the given registry schema. A
     * protobuf schema is compiled together with its references, which only the registry has, so
     * the table carries the schema's id rather than its text.
     */
    public static String readerSchemaReference(int schemaId)
    {
        return READER_SCHEMA_REFERENCE_PREFIX + schemaId;
    }

    private static int parseReaderSchemaReference(String reference)
    {
        checkArgument(reference.startsWith(READER_SCHEMA_REFERENCE_PREFIX), "Data schema is not a schema registry reference: %s", reference);
        try {
            return Integer.parseInt(reference.substring(READER_SCHEMA_REFERENCE_PREFIX.length()));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("Data schema is not a schema registry reference: %s", reference), e);
        }
    }

    @Override
    public Optional<Descriptor> getReaderDescriptor()
    {
        return readerSchemaId.map(schemaId -> getDescriptor(new MessageKey(schemaId, ImmutableList.of(0))));
    }

    @Override
    public DynamicMessage parseDynamicMessage(byte[] data)
    {
        Frame frame = ConfluentProtobufWireFormat.readFrame(data);
        Descriptor descriptor = getDescriptor(new MessageKey(frame.schemaId(), frame.messageIndexes()));
        try {
            return DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(data, frame.payloadOffset(), data.length - frame.payloadOffset()));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.", e);
        }
    }

    private Descriptor getDescriptor(MessageKey key)
    {
        try {
            return descriptorCache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private Descriptor lookupDescriptor(MessageKey key)
    {
        ParsedSchema schema;
        try {
            schema = schemaRegistryClient.getSchemaById(key.schemaId());
        }
        catch (RestClientException e) {
            if (e.getStatus() == HTTP_NOT_FOUND) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s does not exist in the schema registry", key.schemaId()), e);
            }
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up schema id %s in the schema registry: %s", key.schemaId(), e.getMessage()), e);
        }
        catch (IOException e) {
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up schema id %s in the schema registry: %s", key.schemaId(), e.getMessage()), e);
        }
        if (!(schema instanceof ConfluentProtobufSchema protobufSchema)) {
            throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s is a %s schema, not a PROTOBUF schema", key.schemaId(), schema.schemaType()));
        }
        return ConfluentProtobufWireFormat.findMessage(protobufSchema.getFileDescriptor(), key.messageIndexes())
                .orElseThrow(() -> new TrinoException(INVALID_PROTOBUF_MESSAGE, format("Message indexes %s do not exist in schema id %s", key.messageIndexes(), key.schemaId())));
    }

    private record MessageKey(int schemaId, List<Integer> messageIndexes)
    {
        private MessageKey
        {
            messageIndexes = ImmutableList.copyOf(requireNonNull(messageIndexes, "messageIndexes is null"));
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
            return new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient, protoFile.map(ConfluentSchemaRegistryDynamicMessageProvider::parseReaderSchemaReference));
        }
    }
}
