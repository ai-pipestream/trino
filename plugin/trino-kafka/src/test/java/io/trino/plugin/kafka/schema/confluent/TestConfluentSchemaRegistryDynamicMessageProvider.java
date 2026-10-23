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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConfluentSchemaRegistryDynamicMessageProvider
{
    private static final String TOPIC = "test";

    @Test
    public void testFirstMessage()
            throws Exception
    {
        assertRoundTrip(header());
    }

    @Test
    public void testSecondMessage()
            throws Exception
    {
        // The enum declared between the two messages does not count towards the message index
        assertRoundTrip(order());
    }

    @Test
    public void testNestedMessage()
            throws Exception
    {
        // The map entry type generated for Order.attributes does not count towards the message index
        assertRoundTrip(line());
    }

    @Test
    public void testDeeplyNestedMessage()
            throws Exception
    {
        assertRoundTrip(discount());
    }

    @Test
    public void testMessagesSharingSchemaId()
            throws Exception
    {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        DynamicMessageProvider dynamicMessageProvider = new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient);
        List<DynamicMessage> messages = ImmutableList.of(header(), order(), line(), discount(), order(), header());

        // All messages come from one file, so they are written with one schema id and must not share a cache entry
        List<byte[]> records = messages.stream()
                .map(message -> serialize(schemaRegistryClient, message))
                .collect(toImmutableList());
        assertThat(records.stream().map(TestConfluentSchemaRegistryDynamicMessageProvider::schemaId).distinct()).hasSize(1);

        for (int i = 0; i < messages.size(); i++) {
            assertDecoded(dynamicMessageProvider.parseDynamicMessage(records.get(i)), messages.get(i));
        }
    }

    @Test
    public void testEmptyMessageIndex()
            throws Exception
    {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        int schemaId = register(schemaRegistryClient);
        DynamicMessage message = header();

        // A single zero byte in place of the message index refers to the first message
        byte[] record = ByteBuffer.allocate(1 + Integer.BYTES + 1 + message.getSerializedSize())
                .put((byte) 0)
                .putInt(schemaId)
                .put((byte) 0)
                .put(message.toByteArray())
                .array();
        assertDecoded(new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient).parseDynamicMessage(record), message);
    }

    @Test
    public void testMissingMessageIndex()
            throws Exception
    {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        int schemaId = register(schemaRegistryClient);
        DynamicMessageProvider dynamicMessageProvider = new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient);

        for (List<Integer> messageIndex : ImmutableList.<List<Integer>>of(
                ImmutableList.of(2),
                ImmutableList.of(-1),
                ImmutableList.of(0, 0),
                ImmutableList.of(1, 1),
                ImmutableList.of(1, 0, 1),
                ImmutableList.of(1, 0, 0, 0))) {
            assertTrinoExceptionThrownBy(() -> dynamicMessageProvider.parseDynamicMessage(frame(schemaId, messageIndex)))
                    .hasErrorCode(INVALID_PROTOBUF_MESSAGE)
                    .hasMessage("Message index %s does not exist in schema with id %s".formatted(messageIndex, schemaId));
        }
    }

    @Test
    public void testUnknownSchemaId()
    {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        DynamicMessageProvider dynamicMessageProvider = new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient);

        assertTrinoExceptionThrownBy(() -> dynamicMessageProvider.parseDynamicMessage(frame(42, ImmutableList.of(0))))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessage("Looking up schemaId '42' from confluent schema registry failed");
    }

    private static void assertRoundTrip(DynamicMessage message)
            throws Exception
    {
        byte[] record = serialize(new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider())), message);

        // A schema registry serves the schema text, which does not say which message the producer wrote
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        assertThat(register(schemaRegistryClient)).isEqualTo(schemaId(record));
        assertDecoded(new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient).parseDynamicMessage(record), message);
    }

    private static void assertDecoded(DynamicMessage decoded, DynamicMessage expected)
    {
        assertThat(decoded.getDescriptorForType().getFullName()).isEqualTo(expected.getDescriptorForType().getFullName());
        assertThat(decoded.getUnknownFields().asMap()).isEmpty();
        assertThat(decoded.toString()).isEqualTo(expected.toString());
    }

    private static byte[] serialize(MockSchemaRegistryClient schemaRegistryClient, DynamicMessage message)
    {
        try (KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(schemaRegistryClient, ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, "unused"))) {
            return serializer.serialize(TOPIC, message);
        }
    }

    private static int register(MockSchemaRegistryClient schemaRegistryClient)
            throws Exception
    {
        return schemaRegistryClient.register(TOPIC + "-value", new ProtobufSchema(getProtoFile("protobuf/multiple_messages.proto")));
    }

    private static int schemaId(byte[] record)
    {
        return ByteBuffer.wrap(record, 1, Integer.BYTES).getInt();
    }

    private static byte[] frame(int schemaId, List<Integer> messageIndex)
    {
        byte[] indexes = new MessageIndexes(messageIndex).toByteArray();
        return ByteBuffer.allocate(1 + Integer.BYTES + indexes.length)
                .put((byte) 0)
                .putInt(schemaId)
                .put(indexes)
                .array();
    }

    private static DynamicMessage header()
            throws Exception
    {
        Descriptor descriptor = fileDescriptor().findMessageTypeByName("Header");
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("name"), "header")
                .build();
    }

    private static DynamicMessage order()
            throws Exception
    {
        Descriptor descriptor = fileDescriptor().findMessageTypeByName("Order");
        FieldDescriptor attributes = descriptor.findFieldByName("attributes");
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 42L)
                .setField(descriptor.findFieldByName("customer"), "Trino")
                .addRepeatedField(attributes, DynamicMessage.newBuilder(attributes.getMessageType())
                        .setField(attributes.getMessageType().findFieldByName("key"), "channel")
                        .setField(attributes.getMessageType().findFieldByName("value"), "web")
                        .build())
                .setField(descriptor.findFieldByName("status"), descriptor.findFieldByName("status").getEnumType().findValueByName("SHIPPED"))
                .build();
    }

    private static DynamicMessage line()
            throws Exception
    {
        Descriptor descriptor = fileDescriptor().findMessageTypeByName("Order").findNestedTypeByName("Line");
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("sku"), "sku-1")
                .setField(descriptor.findFieldByName("quantity"), 3)
                .build();
    }

    private static DynamicMessage discount()
            throws Exception
    {
        Descriptor descriptor = fileDescriptor().findMessageTypeByName("Order").findNestedTypeByName("Line").findNestedTypeByName("Discount");
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("code"), "SPRING")
                .setField(descriptor.findFieldByName("percent"), 12.5)
                .build();
    }

    private static FileDescriptor fileDescriptor()
            throws Exception
    {
        return getFileDescriptor(getProtoFile("protobuf/multiple_messages.proto"));
    }
}
