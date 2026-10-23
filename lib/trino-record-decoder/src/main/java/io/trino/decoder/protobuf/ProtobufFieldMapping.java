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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import jakarta.annotation.Nullable;

import java.util.Optional;

/**
 * Maps the field a column names in the reader schema (the schema the table columns were derived
 * from) to the field of the schema a record was written with. A field is identified by its
 * number on the wire, so the writer field is the one with the reader field's number, provided
 * it is encoded the same way; a renamed field keeps its column, and a column whose field the
 * writer does not have, or has reused for a different kind of value, is {@code NULL}.
 */
final class ProtobufFieldMapping
{
    private ProtobufFieldMapping() {}

    /**
     * The writer field a column path element selects, or {@code null} if the writer has no
     * compatible field for it. Without a reader type, or when the reader does not declare the
     * name (which is the case for a {@code oneof} column), the name identifies the field.
     */
    @Nullable
    static FieldDescriptor findWriterField(Descriptor writerType, Optional<Descriptor> readerType, String name)
    {
        FieldDescriptor readerField = readerType.map(type -> type.findFieldByName(name)).orElse(null);
        if (readerField == null) {
            return writerType.findFieldByName(name);
        }
        FieldDescriptor writerField = writerType.findFieldByNumber(readerField.getNumber());
        if (writerField == null || !isSameEncoding(readerField, writerField)) {
            return null;
        }
        return writerField;
    }

    /**
     * The reader type of the values of a message-typed field, for mapping the fields nested in it.
     */
    static Optional<Descriptor> getReaderFieldType(Optional<Descriptor> readerType, String name)
    {
        return readerType.map(type -> type.findFieldByName(name))
                .filter(field -> field.getJavaType() == JavaType.MESSAGE)
                .map(FieldDescriptor::getMessageType);
    }

    private static boolean isSameEncoding(FieldDescriptor readerField, FieldDescriptor writerField)
    {
        return readerField.isRepeated() == writerField.isRepeated()
                && readerField.isMapField() == writerField.isMapField()
                && encoding(readerField.getType()) == encoding(writerField.getType());
    }

    /**
     * Field types that can replace each other without changing how the field's values are encoded,
     * per the "Updating A Message Type" rules of the protobuf language guide.
     */
    private static Encoding encoding(FieldDescriptor.Type type)
    {
        return switch (type) {
            case INT32, INT64, UINT32, UINT64, BOOL, ENUM -> Encoding.VARINT;
            case SINT32, SINT64 -> Encoding.ZIGZAG_VARINT;
            case STRING, BYTES -> Encoding.LENGTH_DELIMITED;
            case FIXED32, SFIXED32 -> Encoding.FIXED32;
            case FIXED64, SFIXED64 -> Encoding.FIXED64;
            case FLOAT -> Encoding.FLOAT;
            case DOUBLE -> Encoding.DOUBLE;
            case MESSAGE -> Encoding.MESSAGE;
            case GROUP -> Encoding.GROUP;
        };
    }

    private enum Encoding
    {
        VARINT,
        ZIGZAG_VARINT,
        LENGTH_DELIMITED,
        FIXED32,
        FIXED64,
        FLOAT,
        DOUBLE,
        MESSAGE,
        GROUP,
    }
}
