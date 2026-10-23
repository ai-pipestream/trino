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
package io.trino.plugin.kafka.protobuf;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufSchemaParser;
import io.trino.plugin.kafka.schema.ProtobufAnySupportConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestProtobufSchemaParser
{
    @Test
    public void testTopicNameStrategySubject()
            throws Exception
    {
        // The subject does not name a message, so the first message is used
        assertThat(parse("orders-value")).containsExactly("name");
        assertThat(parse("orders-key")).containsExactly("name");
    }

    @Test
    public void testRecordNameStrategySubject()
            throws Exception
    {
        assertThat(parse("io.trino.protobuf.Header")).containsExactly("name");
        assertThat(parse("io.trino.protobuf.Order")).containsExactly("id", "customer", "attributes", "status");
        assertThat(parse("io.trino.protobuf.Order.Line")).containsExactly("sku", "quantity");
        assertThat(parse("io.trino.protobuf.Order.Line.Discount")).containsExactly("code", "percent");
    }

    @Test
    public void testTopicRecordNameStrategySubject()
            throws Exception
    {
        assertThat(parse("orders-io.trino.protobuf.Order")).containsExactly("id", "customer", "attributes", "status");
        assertThat(parse("orders-io.trino.protobuf.Order.Line")).containsExactly("sku", "quantity");
        assertThat(parse("my-orders-io.trino.protobuf.Order.Line.Discount")).containsExactly("code", "percent");
    }

    @Test
    public void testSubjectNamingUnknownMessage()
            throws Exception
    {
        assertThat(parse("io.trino.protobuf.Missing")).containsExactly("name");
        assertThat(parse("orders-io.trino.protobuf.Missing")).containsExactly("name");
        // Only the full name of a message identifies it
        assertThat(parse("Order")).containsExactly("name");
        assertThat(parse("orders-Order")).containsExactly("name");
        assertThat(parse("io.trino.protobuf.Order-value")).containsExactly("name");
        assertThat(parse("orders-xio.trino.protobuf.Order")).containsExactly("name");
    }

    private static List<String> parse(String subject)
            throws Exception
    {
        ProtobufSchema schema = new ProtobufSchema(getProtoFile("protobuf/multiple_messages.proto"));
        return new ProtobufSchemaParser(TESTING_TYPE_MANAGER, new ProtobufAnySupportConfig())
                .parse(SESSION, subject, schema)
                .fields().stream()
                .map(KafkaTopicFieldDescription::name)
                .toList();
    }
}
