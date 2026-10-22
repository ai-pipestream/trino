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
package io.trino.decoder.protobuf;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtoFileDescriptorBuilder
{
    private static final ProtoFileDescriptorBuilder.ImportResolver NO_IMPORTS = _ -> Optional.empty();

    @Test
    public void testScalarsLabelsAndEnums()
    {
        FileDescriptor file = build("scalars.proto",
                """
                syntax = "proto2";
                package test;
                enum Color {
                  option allow_alias = true;
                  RED = 0;
                  CRIMSON = 0;
                  BLUE = 1;
                }
                message Scalars {
                  required int32 a = 1;
                  optional string b = 2 [default = "x"];
                  repeated sint64 c = 3 [packed = true];
                  optional Color color = 4 [default = BLUE];
                  optional bytes d = 5;
                  optional double e = 6 [json_name = "ee"];
                }
                """);
        assertThat(file.toProto().getSyntax()).isEqualTo("proto2");
        assertThat(file.getPackage()).isEqualTo("test");

        Descriptor scalars = file.findMessageTypeByName("Scalars");
        assertThat(scalars.findFieldByName("a").isRequired()).isTrue();
        assertThat(scalars.findFieldByName("a").getType()).isEqualTo(FieldDescriptor.Type.INT32);
        assertThat(scalars.findFieldByName("b").getDefaultValue()).isEqualTo("x");
        FieldDescriptor packed = scalars.findFieldByName("c");
        assertThat(packed.isRepeated()).isTrue();
        assertThat(packed.isPacked()).isTrue();
        assertThat(packed.getType()).isEqualTo(FieldDescriptor.Type.SINT64);
        FieldDescriptor color = scalars.findFieldByName("color");
        assertThat(color.getJavaType()).isEqualTo(JavaType.ENUM);
        assertThat(color.getEnumType().findValueByName("CRIMSON").getNumber()).isEqualTo(0);
        assertThat(color.getDefaultValue()).isEqualTo(color.getEnumType().findValueByName("BLUE"));
        assertThat(scalars.findFieldByName("d").getType()).isEqualTo(FieldDescriptor.Type.BYTES);
        assertThat(scalars.findFieldByName("e").getJsonName()).isEqualTo("ee");
    }

    @Test
    public void testOneofsNestedTypesAndMaps()
    {
        FileDescriptor file = build("nested.proto",
                """
                syntax = "proto3";
                package test;
                message Outer {
                  message Inner {
                    string value = 1;
                    Deep deep = 2;
                    message Deep {
                      int32 depth = 1;
                    }
                  }
                  enum Kind {
                    UNKNOWN = 0;
                    KNOWN = 1;
                  }
                  string name = 1;
                  oneof choice {
                    Inner inner = 2;
                    Inner.Deep deep = 3;
                    Kind kind = 4;
                  }
                  map<string, Inner> by_name = 5;
                  map<int64, string> labels = 6;
                  repeated Inner inners = 7;
                  optional int32 maybe = 8;
                  message Later {
                    Outer outer = 1;
                  }
                }
                message Second {
                  Outer.Inner.Deep deep = 1;
                }
                """);
        assertThat(file.toProto().getSyntax()).isEqualTo("proto3");
        Descriptor outer = file.findMessageTypeByName("Outer");
        assertThat(outer.getFields().stream().map(FieldDescriptor::getName))
                .containsExactly("name", "by_name", "labels", "inners", "maybe", "inner", "deep", "kind");

        // declared nested messages keep their source order, map entries follow
        assertThat(outer.getNestedTypes().stream().map(Descriptor::getName))
                .containsExactly("Inner", "Later", "ByNameEntry", "LabelsEntry");
        assertThat(outer.findNestedTypeByName("ByNameEntry").getOptions().getMapEntry()).isTrue();
        assertThat(outer.findNestedTypeByName("Later").getOptions().getMapEntry()).isFalse();

        assertThat(outer.getRealOneofs()).hasSize(1);
        assertThat(outer.getRealOneofs().getFirst().getName()).isEqualTo("choice");
        assertThat(outer.findFieldByName("inner").getContainingOneof().getName()).isEqualTo("choice");
        assertThat(outer.findFieldByName("deep").getMessageType().getFullName()).isEqualTo("test.Outer.Inner.Deep");
        assertThat(outer.findFieldByName("kind").getJavaType()).isEqualTo(JavaType.ENUM);

        FieldDescriptor byName = outer.findFieldByName("by_name");
        assertThat(byName.isMapField()).isTrue();
        assertThat(byName.getMessageType().findFieldByName("value").getMessageType().getName()).isEqualTo("Inner");
        assertThat(outer.findFieldByName("labels").getMessageType().findFieldByName("key").getType()).isEqualTo(FieldDescriptor.Type.INT64);
        assertThat(outer.findFieldByName("inners").isRepeated()).isTrue();

        FieldDescriptor maybe = outer.findFieldByName("maybe");
        assertThat(maybe.toProto().getProto3Optional()).isTrue();
        assertThat(maybe.getContainingOneof().getName()).isEqualTo("_maybe");
        assertThat(maybe.getContainingOneof().getFieldCount()).isEqualTo(1);
        assertThat(outer.getOneofs()).hasSize(2);

        // types referenced before their declaration and across messages resolve
        assertThat(outer.findNestedTypeByName("Later").findFieldByName("outer").getMessageType()).isSameAs(outer);
        assertThat(file.findMessageTypeByName("Second").findFieldByName("deep").getMessageType().getFullName()).isEqualTo("test.Outer.Inner.Deep");
    }

    @Test
    public void testWellKnownTypesLinkAgainstBundledDescriptors()
    {
        FileDescriptor file = build("wkt.proto",
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                import "google/protobuf/any.proto";
                import "google/protobuf/struct.proto";
                message Event {
                  google.protobuf.Timestamp at = 1;
                  google.protobuf.Any payload = 2;
                  google.protobuf.Struct extra = 3;
                }
                """);
        Descriptor event = file.findMessageTypeByName("Event");
        assertThat(event.findFieldByName("at").getMessageType()).isSameAs(Timestamp.getDescriptor());
        assertThat(file.getDependencies()).contains(TimestampProto.getDescriptor());
    }

    @Test
    public void testImportsResolveThroughResolverAndAreBuiltOnce()
    {
        Map<String, String> files = ImmutableMap.of(
                "common/money.proto",
                """
                syntax = "proto3";
                package common;
                message Money {
                  string currency = 1;
                  int64 units = 2;
                }
                """,
                "orders/line.proto",
                """
                syntax = "proto3";
                package orders;
                import "common/money.proto";
                message Line {
                  common.Money price = 1;
                }
                """);
        FileDescriptor file = build(
                "orders/order.proto",
                """
                syntax = "proto3";
                package orders;
                import "common/money.proto";
                import "orders/line.proto";
                message Order {
                  repeated Line lines = 1;
                  .common.Money total = 2;
                }
                """,
                importPath -> Optional.ofNullable(files.get(importPath)));
        Descriptor order = file.findMessageTypeByName("Order");
        Descriptor money = order.findFieldByName("total").getMessageType();
        assertThat(money.getFullName()).isEqualTo("common.Money");
        // the diamond import is a single descriptor instance
        assertThat(order.findFieldByName("lines").getMessageType().findFieldByName("price").getMessageType()).isSameAs(money);
        assertThat(file.getDependencies().stream().map(FileDescriptor::getName)).containsExactly("common/money.proto", "orders/line.proto");
    }

    @Test
    public void testPublicImportsAreTransitive()
    {
        Map<String, String> files = ImmutableMap.of(
                "a.proto",
                """
                syntax = "proto3";
                message A {
                  int32 x = 1;
                }
                """,
                "b.proto",
                """
                syntax = "proto3";
                import public "a.proto";
                """);
        FileDescriptor file = build(
                "c.proto",
                """
                syntax = "proto3";
                import "b.proto";
                message C {
                  A a = 1;
                }
                """,
                importPath -> Optional.ofNullable(files.get(importPath)));
        assertThat(file.findMessageTypeByName("C").findFieldByName("a").getMessageType().getFullName()).isEqualTo("A");
    }

    @Test
    public void testUnresolvedImport()
    {
        assertThatThrownBy(() -> build("x.proto",
                """
                syntax = "proto3";
                import "missing.proto";
                message X {}
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Import 'missing.proto' of protobuf schema 'x.proto' could not be resolved")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(INVALID_PROTO_FILE.toErrorCode()));
    }

    @Test
    public void testImportCycle()
    {
        Map<String, String> files = ImmutableMap.of(
                "a.proto",
                """
                syntax = "proto3";
                import "b.proto";
                """,
                "b.proto",
                """
                syntax = "proto3";
                import "a.proto";
                """);
        assertThatThrownBy(() -> build("a.proto", files.get("a.proto"), importPath -> Optional.ofNullable(files.get(importPath))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Import cycle in protobuf schema: a.proto -> b.proto -> a.proto");
    }

    @Test
    public void testUnknownTypeAndParseErrorsFailLoudly()
    {
        assertThatThrownBy(() -> build("x.proto",
                """
                syntax = "proto3";
                message X {
                  Missing m = 1;
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'x.proto': ")
                .hasMessageContaining("\"Missing\" is not defined");

        assertThatThrownBy(() -> build("x.proto", "message X { int32 = 1; }"))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Failed to parse protobuf schema 'x.proto': ");
    }

    @Test
    public void testMissingSyntaxIsProto2()
    {
        FileDescriptor file = build("legacy.proto",
                """
                message Legacy {
                  optional string name = 1;
                  repeated int32 values = 2;
                }
                """);
        assertThat(file.toProto().hasSyntax()).isFalse();
        FieldDescriptor name = file.findMessageTypeByName("Legacy").findFieldByName("name");
        assertThat(name.hasPresence()).isTrue();
        assertThat(file.findMessageTypeByName("Legacy").findFieldByName("values").isPacked()).isFalse();
    }

    @Test
    public void testProto3FieldsHaveNoPresenceUnlessOptional()
    {
        FileDescriptor file = build("presence.proto",
                """
                syntax = "proto3";
                message Presence {
                  string implicit = 1;
                  optional string explicit = 2;
                  repeated int32 values = 3;
                  Presence nested = 4;
                }
                """);
        Descriptor presence = file.findMessageTypeByName("Presence");
        assertThat(presence.findFieldByName("implicit").hasPresence()).isFalse();
        assertThat(presence.findFieldByName("explicit").hasPresence()).isTrue();
        assertThat(presence.findFieldByName("explicit").getContainingOneof().getName()).isEqualTo("_explicit");
        assertThat(presence.getRealOneofs()).isEmpty();
        assertThat(presence.findFieldByName("values").isPacked()).isTrue();
        assertThat(presence.findFieldByName("nested").hasPresence()).isTrue();

        // a proto3 implicit-presence field set to its default value is not serialized
        DynamicMessage message = DynamicMessage.newBuilder(presence)
                .setField(presence.findFieldByName("implicit"), "")
                .setField(presence.findFieldByName("explicit"), "")
                .build();
        assertThat(message.toByteArray()).containsExactly(0x12, 0x00);
    }

    @Test
    public void testDeclarationsWithoutDescriptorsAreIgnored()
    {
        FileDescriptor file = build("ignored.proto",
                """
                syntax = "proto2";
                package test;
                import "google/protobuf/descriptor.proto";
                option java_package = "com.example";
                option (my_file_option) = "x";
                extend google.protobuf.FieldOptions {
                  optional string my_option = 51234;
                }
                message Settings {
                  option deprecated = true;
                  option (my_message_option) = 7;
                  reserved 2, 15 to 20;
                  reserved "old_name", "older_name";
                  extensions 100 to 199;
                  optional string name = 1 [(my_option) = "value", deprecated = true];
                  optional int32 count = 3 [(my_option) = "value"];
                }
                service Api {
                  rpc Get (Settings) returns (Settings);
                }
                """);
        Descriptor settings = file.findMessageTypeByName("Settings");
        assertThat(settings.getFields()).extracting(FieldDescriptor::getName).containsExactly("name", "count");
        assertThat(settings.findFieldByName("name").getOptions().getDeprecated()).isTrue();
        assertThat(settings.findFieldByName("count").getOptions().getDeprecated()).isFalse();
        assertThat(file.getServices()).isEmpty();
        assertThat(file.getExtensions()).isEmpty();
    }

    @Test
    public void testTypeNameResolution()
    {
        FileDescriptor file = build("names.proto",
                """
                syntax = "proto3";
                package outer.inner;
                message Outer {
                  enum Kind {
                    NONE = 0;
                    SOME = 1;
                  }
                  message Nested {
                    Kind kind = 1;
                    Outer parent = 2;
                    .outer.inner.Outer qualified_parent = 3;
                    Sibling sibling = 4;
                    outer.inner.Sibling.Deep deep = 5;
                  }
                  Nested nested = 1;
                  Outer.Nested qualified_nested = 2;
                  Sibling.Deep deep = 3;
                }
                message Sibling {
                  message Deep {
                    Outer.Kind kind = 1;
                  }
                  Outer.Nested nested = 1;
                }
                """);
        Descriptor outer = file.findMessageTypeByName("Outer");
        Descriptor nested = outer.findNestedTypeByName("Nested");
        Descriptor sibling = file.findMessageTypeByName("Sibling");
        Descriptor deep = sibling.findNestedTypeByName("Deep");

        assertThat(nested.findFieldByName("kind").getEnumType()).isSameAs(outer.findEnumTypeByName("Kind"));
        assertThat(nested.findFieldByName("parent").getMessageType()).isSameAs(outer);
        assertThat(nested.findFieldByName("qualified_parent").getMessageType()).isSameAs(outer);
        assertThat(nested.findFieldByName("sibling").getMessageType()).isSameAs(sibling);
        assertThat(nested.findFieldByName("deep").getMessageType()).isSameAs(deep);
        assertThat(outer.findFieldByName("nested").getMessageType()).isSameAs(nested);
        assertThat(outer.findFieldByName("qualified_nested").getMessageType()).isSameAs(nested);
        assertThat(outer.findFieldByName("deep").getMessageType()).isSameAs(deep);
        assertThat(deep.findFieldByName("kind").getEnumType()).isSameAs(outer.findEnumTypeByName("Kind"));
        assertThat(sibling.findFieldByName("nested").getMessageType()).isSameAs(nested);
        assertThat(deep.getFullName()).isEqualTo("outer.inner.Sibling.Deep");
    }

    @Test
    public void testTypesFromImportedPackages()
    {
        Map<String, String> imports = ImmutableMap.of(
                "common/address.proto",
                """
                syntax = "proto3";
                package common;
                message Address {
                  string city = 1;
                }
                enum Region {
                  NORTH = 0;
                }
                """,
                "common/person.proto",
                """
                syntax = "proto3";
                package common;
                import "common/address.proto";
                message Person {
                  Address address = 1;
                  Region region = 2;
                }
                """);
        FileDescriptor file = build(
                "order.proto",
                """
                syntax = "proto3";
                package shop;
                import "common/person.proto";
                import "common/address.proto";
                message Order {
                  common.Person customer = 1;
                  common.Address destination = 2;
                  .common.Region region = 3;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath)));
        Descriptor order = file.findMessageTypeByName("Order");
        Descriptor person = order.findFieldByName("customer").getMessageType();
        assertThat(person.getFullName()).isEqualTo("common.Person");
        // both files see the same Address descriptor
        assertThat(order.findFieldByName("destination").getMessageType()).isSameAs(person.findFieldByName("address").getMessageType());
        assertThat(order.findFieldByName("region").getEnumType()).isSameAs(person.findFieldByName("region").getEnumType());
        assertThat(file.getDependencies()).extracting(FileDescriptor::getName).containsExactly("common/person.proto", "common/address.proto");
    }

    @Test
    public void testTypeFromTransitiveImportIsNotVisible()
    {
        Map<String, String> imports = ImmutableMap.of(
                "address.proto",
                """
                syntax = "proto3";
                message Address {
                  string city = 1;
                }
                """,
                "person.proto",
                """
                syntax = "proto3";
                import "address.proto";
                message Person {
                  Address address = 1;
                }
                """);
        assertThatThrownBy(() -> build(
                "order.proto",
                """
                syntax = "proto3";
                import "person.proto";
                message Order {
                  Address destination = 1;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath))))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'order.proto': ")
                .hasMessageContaining("\"Address\"");
    }

    @Test
    public void testMapEntryNamesFollowProtoc()
    {
        FileDescriptor file = build("maps.proto",
                """
                syntax = "proto3";
                message Maps {
                  map<string, int32> simple = 1;
                  map<string, Maps> nested_map = 2;
                  map<int64, string> _leading = 3;
                  map<string, string> a_b_c = 4;
                  map<string, string> already_Entry = 5;
                  message Declared {
                    string unrelated = 1;
                  }
                  Declared declared = 6;
                }
                """);
        Descriptor maps = file.findMessageTypeByName("Maps");
        assertThat(maps.findFieldByName("simple").getMessageType().getName()).isEqualTo("SimpleEntry");
        assertThat(maps.findFieldByName("_leading").getMessageType().getName()).isEqualTo("LeadingEntry");
        assertThat(maps.findFieldByName("a_b_c").getMessageType().getName()).isEqualTo("ABCEntry");
        assertThat(maps.findFieldByName("already_Entry").getMessageType().getName()).isEqualTo("AlreadyEntryEntry");
        FieldDescriptor nestedMap = maps.findFieldByName("nested_map");
        assertThat(nestedMap.isMapField()).isTrue();
        assertThat(nestedMap.getMessageType().findFieldByName("value").getMessageType()).isSameAs(maps);
        assertThat(maps.findFieldByName("declared").getMessageType().getOptions().getMapEntry()).isFalse();
        // declared nested messages come first, in source order, followed by the map entry types
        assertThat(maps.getNestedTypes()).extracting(Descriptor::getName).containsExactly("Declared", "SimpleEntry", "NestedMapEntry", "LeadingEntry", "ABCEntry", "AlreadyEntryEntry");

        // a declared message named like a map entry collides, as it does with protoc
        assertThatThrownBy(() -> build("collision.proto",
                """
                syntax = "proto3";
                message Maps {
                  map<string, string> nested_map = 1;
                  message NestedMapEntry {}
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid protobuf schema 'collision.proto': Maps.NestedMapEntry: \"NestedMapEntry\" is already defined in \"Maps\".");
    }

    @Test
    public void testGroupsAreRejected()
    {
        assertThatThrownBy(() -> build("groups.proto",
                """
                syntax = "proto2";
                message Legacy {
                  optional group Result = 1 {
                    optional string url = 2;
                  }
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Groups are not supported: message 'Legacy' in protobuf schema 'groups.proto'");
    }

    @Test
    public void testDuplicateDeclarationsAreRejected()
    {
        assertThatThrownBy(() -> build("dup.proto",
                """
                syntax = "proto3";
                message Dup {
                  string a = 1;
                  string b = 1;
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'dup.proto': ")
                .hasMessageContaining("Field number 1 has already been used");

        assertThatThrownBy(() -> build("dup.proto",
                """
                syntax = "proto3";
                message Dup {}
                message Dup {}
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Failed to parse protobuf schema 'dup.proto': Dup (dup.proto:3:1) is already defined at dup.proto:2:1");
    }

    @Test
    public void testDescriptorsRoundTripMessages()
            throws Exception
    {
        FileDescriptor file = build("rt.proto",
                """
                syntax = "proto3";
                message Pair {
                  string key = 1;
                  oneof value {
                    int64 number = 2;
                    string text = 3;
                  }
                  map<string, int32> counts = 4;
                }
                """);
        Descriptor pair = file.findMessageTypeByName("Pair");
        Descriptor entry = pair.findFieldByName("counts").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(pair)
                .setField(pair.findFieldByName("key"), "k")
                .setField(pair.findFieldByName("text"), "t")
                .addRepeatedField(pair.findFieldByName("counts"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "a")
                        .setField(entry.findFieldByName("value"), 7)
                        .build())
                .build();
        DynamicMessage parsed = DynamicMessage.parseFrom(pair, message.toByteArray());
        assertThat(parsed).isEqualTo(message);
        assertThat(parsed.getOneofFieldDescriptor(pair.getOneofs().getFirst()).getName()).isEqualTo("text");
    }

    private static FileDescriptor build(String fileName, String source)
    {
        return build(fileName, source, NO_IMPORTS);
    }

    private static FileDescriptor build(String fileName, String source, ProtoFileDescriptorBuilder.ImportResolver importResolver)
    {
        return ProtoFileDescriptorBuilder.buildFileDescriptor(fileName, source, importResolver);
    }
}
