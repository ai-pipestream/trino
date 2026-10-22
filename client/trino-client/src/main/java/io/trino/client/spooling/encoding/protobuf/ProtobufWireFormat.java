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
package io.trino.client.spooling.encoding.protobuf;

/**
 * Field numbers and wire types of the protobuf spooled segment format,
 * mirroring {@code trino_spooled_segment.proto}.
 */
public final class ProtobufWireFormat
{
    private ProtobufWireFormat() {}

    // Wire types
    public static final int WIRETYPE_VARINT = 0;
    public static final int WIRETYPE_FIXED64 = 1;
    public static final int WIRETYPE_LENGTH_DELIMITED = 2;
    public static final int WIRETYPE_FIXED32 = 5;

    // Segment
    public static final int SEGMENT_ROWS = 1;

    // Row
    public static final int ROW_VALUES = 1;

    // Value.kind
    public static final int VALUE_NULL = 1;
    public static final int VALUE_BOOLEAN = 2;
    public static final int VALUE_TINYINT = 3;
    public static final int VALUE_SMALLINT = 4;
    public static final int VALUE_INTEGER = 5;
    public static final int VALUE_BIGINT = 6;
    public static final int VALUE_REAL = 7;
    public static final int VALUE_DOUBLE = 8;
    public static final int VALUE_DECIMAL = 9;
    public static final int VALUE_VARCHAR = 10;
    public static final int VALUE_CHAR = 11;
    public static final int VALUE_VARBINARY = 12;
    public static final int VALUE_DATE = 13;
    public static final int VALUE_TIME = 14;
    public static final int VALUE_TIME_WITH_TIME_ZONE = 15;
    public static final int VALUE_TIMESTAMP = 16;
    public static final int VALUE_TIMESTAMP_WITH_TIME_ZONE = 17;
    public static final int VALUE_INTERVAL_YEAR_TO_MONTH = 18;
    public static final int VALUE_INTERVAL_DAY_TO_SECOND = 19;
    public static final int VALUE_JSON = 20;
    public static final int VALUE_UUID = 21;
    public static final int VALUE_IPADDRESS = 22;
    public static final int VALUE_NUMBER = 23;
    public static final int VALUE_VARIANT = 24;
    public static final int VALUE_ARRAY = 25;
    public static final int VALUE_MAP = 26;
    public static final int VALUE_ROW = 27;
    public static final int VALUE_TEXT = 28;
    public static final int VALUE_JSON_OBJECT = 29;

    // NullValue
    public static final int NULL_VALUE = 0;

    // Decimal
    public static final int DECIMAL_UNSCALED_LONG = 1;
    public static final int DECIMAL_UNSCALED_BYTES = 2;
    public static final int DECIMAL_SCALE = 3;

    // Time
    public static final int TIME_PICOS = 1;
    public static final int TIME_PRECISION = 2;

    // TimeWithTimeZone
    public static final int TIME_WITH_TIME_ZONE_PICOS = 1;
    public static final int TIME_WITH_TIME_ZONE_PRECISION = 2;
    public static final int TIME_WITH_TIME_ZONE_OFFSET_MINUTES = 3;

    // Timestamp
    public static final int TIMESTAMP_EPOCH_MICROS = 1;
    public static final int TIMESTAMP_PICOS_OF_MICRO = 2;
    public static final int TIMESTAMP_PRECISION = 3;

    // TimestampWithTimeZone
    public static final int TIMESTAMP_WITH_TIME_ZONE_EPOCH_MILLIS = 1;
    public static final int TIMESTAMP_WITH_TIME_ZONE_PICOS_OF_MILLI = 2;
    public static final int TIMESTAMP_WITH_TIME_ZONE_PRECISION = 3;
    public static final int TIMESTAMP_WITH_TIME_ZONE_ZONE_ID = 4;
    public static final int TIMESTAMP_WITH_TIME_ZONE_OFFSET_SECONDS = 5;

    // IntervalDayToSecond
    public static final int INTERVAL_DAY_TO_SECOND_MICROS = 1;
    public static final int INTERVAL_DAY_TO_SECOND_PICOS_OF_MICRO = 2;
    public static final int INTERVAL_DAY_TO_SECOND_FRACTIONAL_PRECISION = 3;

    // Number
    public static final int NUMBER_DECIMAL = 1;
    public static final int NUMBER_SPECIAL = 2;
    public static final int NUMBER_SPECIAL_NOT_A_NUMBER = 0;
    public static final int NUMBER_SPECIAL_POSITIVE_INFINITY = 1;
    public static final int NUMBER_SPECIAL_NEGATIVE_INFINITY = 2;

    // Variant
    public static final int VARIANT_BINARY = 1;
    public static final int VARIANT_JSON = 2;

    // VariantBinary
    public static final int VARIANT_BINARY_METADATA = 1;
    public static final int VARIANT_BINARY_VALUE = 2;

    // Array
    public static final int ARRAY_ELEMENTS = 1;

    // Map
    public static final int MAP_KEYS = 1;
    public static final int MAP_VALUES = 2;

    public static int makeTag(int fieldNumber, int wireType)
    {
        return (fieldNumber << 3) | wireType;
    }

    public static int fieldNumber(int tag)
    {
        return tag >>> 3;
    }

    public static int wireType(int tag)
    {
        return tag & 0x7;
    }
}
