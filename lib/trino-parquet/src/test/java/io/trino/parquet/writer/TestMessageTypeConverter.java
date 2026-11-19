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
package io.trino.parquet.writer;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.parquet.writer.MessageTypeConverter.toParquetSchema;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageTypeConverter
{
    @Test
    public void testDecimalPrecisionAndScale()
    {
        MessageType schema = Types.buildMessage()
                .optional(INT32).as(decimalType(2, 9)).named("int32_decimal")
                .optional(INT64).as(decimalType(0, 18)).named("int64_decimal")
                .optional(FIXED_LEN_BYTE_ARRAY).length(16).as(decimalType(38, 38)).named("fixed_decimal")
                .optional(BINARY).as(decimalType(10, 20)).named("binary_decimal")
                .named("test");

        List<SchemaElement> elements = toParquetSchema(schema);
        assertThat(elements).hasSize(5);
        assertDecimal(elements.get(1), "int32_decimal", 9, 2);
        assertDecimal(elements.get(2), "int64_decimal", 18, 0);
        assertDecimal(elements.get(3), "fixed_decimal", 38, 38);
        assertThat(elements.get(3).getType_length()).isEqualTo(16);
        assertDecimal(elements.get(4), "binary_decimal", 20, 10);
    }

    @Test
    public void testNestedDecimalPrecisionAndScale()
    {
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                .optional(INT32).as(decimalType(2, 9)).named("int32_decimal")
                .optionalList()
                .optionalElement(FIXED_LEN_BYTE_ARRAY).length(16).as(decimalType(3, 38)).named("fixed_decimals")
                .named("nested")
                .named("test");

        List<SchemaElement> elements = toParquetSchema(schema);
        assertThat(elements).extracting(SchemaElement::getName).containsExactly("test", "nested", "int32_decimal", "fixed_decimals", "list", "element");
        assertThat(elements.get(1).isSetPrecision()).isFalse();
        assertDecimal(elements.get(2), "int32_decimal", 9, 2);
        assertThat(elements.get(3).isSetPrecision()).isFalse();
        assertThat(elements.get(4).isSetPrecision()).isFalse();
        assertDecimal(elements.get(5), "element", 38, 3);
        assertThat(elements.get(5).getType_length()).isEqualTo(16);
    }

    @Test
    public void testNonDecimalHasNoPrecisionAndScale()
    {
        MessageType schema = Types.buildMessage()
                .optional(INT32).named("int32")
                .optional(INT64).named("int64")
                .optional(BINARY).as(stringType()).named("string")
                .optional(FIXED_LEN_BYTE_ARRAY).length(16).named("fixed")
                .named("test");

        List<SchemaElement> elements = toParquetSchema(schema);
        assertThat(elements).hasSize(5);
        for (SchemaElement element : elements.subList(1, elements.size())) {
            assertThat(element.isSetPrecision()).as(element.getName()).isFalse();
            assertThat(element.isSetScale()).as(element.getName()).isFalse();
        }
    }

    private static void assertDecimal(SchemaElement element, String name, int precision, int scale)
    {
        assertThat(element.getName()).isEqualTo(name);
        assertThat(element.getConverted_type()).isEqualTo(ConvertedType.DECIMAL);
        assertThat(element.getLogicalType().isSetDECIMAL()).isTrue();
        assertThat(element.getLogicalType().getDECIMAL().getPrecision()).isEqualTo(precision);
        assertThat(element.getLogicalType().getDECIMAL().getScale()).isEqualTo(scale);
        assertThat(element.isSetPrecision()).isTrue();
        assertThat(element.getPrecision()).isEqualTo(precision);
        assertThat(element.isSetScale()).isTrue();
        assertThat(element.getScale()).isEqualTo(scale);
    }
}
