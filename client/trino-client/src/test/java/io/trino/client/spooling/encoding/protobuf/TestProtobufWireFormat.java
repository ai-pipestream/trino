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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.trino.client.spooling.encoding.protobuf.generated.Decimal;
import io.trino.client.spooling.encoding.protobuf.generated.NullValue;
import io.trino.client.spooling.encoding.protobuf.generated.Row;
import io.trino.client.spooling.encoding.protobuf.generated.Segment;
import io.trino.client.spooling.encoding.protobuf.generated.Value;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED32;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED64;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_VARINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.fieldNumber;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.makeTag;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.wireType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks the hand-written reader and writer against the protobuf reference implementation.
 */
public class TestProtobufWireFormat
{
    private static final long[] VARINT_SAMPLES = {
            0, 1, 127, 128, 255, 256, 300, 16_383, 16_384, 2_097_151, 2_097_152,
            Integer.MAX_VALUE, 1L << 31, 1L << 32, 1L << 35, 1L << 56, Long.MAX_VALUE, -1, Long.MIN_VALUE,
    };

    @Test
    public void testVarintMatchesReference()
            throws IOException
    {
        for (long value : VARINT_SAMPLES) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeVarint(value);
            assertThat(bytes(writer)).isEqualTo(referenceVarint(value));

            ProtobufWireReader reader = reader(bytes(writer));
            assertThat(reader.readVarint64()).isEqualTo(value);
            assertThat(reader.hasRemaining()).isFalse();
        }
    }

    @Test
    public void testZigZagMatchesReference()
            throws IOException
    {
        for (long value : VARINT_SAMPLES) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeSint64(1, value);
            assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeSInt64(1, value)));

            ProtobufWireReader reader = reader(bytes(writer));
            assertThat(reader.readTag()).isEqualTo(makeTag(1, WIRETYPE_VARINT));
            assertThat(reader.readSint64()).isEqualTo(value);
        }
        for (int value : new int[] {0, -1, 1, -2, 2, 63, -64, 64, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeSint32(1, value);
            assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeSInt32(1, value)));

            ProtobufWireReader reader = reader(bytes(writer));
            reader.readTag();
            assertThat(reader.readSint32()).isEqualTo(value);
        }
    }

    @Test
    public void testZigZagExamplesFromTheSpecification()
    {
        assertThat(ProtobufWireWriter.zigZagEncode(0)).isEqualTo(0);
        assertThat(ProtobufWireWriter.zigZagEncode(-1)).isEqualTo(1);
        assertThat(ProtobufWireWriter.zigZagEncode(1)).isEqualTo(2);
        assertThat(ProtobufWireWriter.zigZagEncode(-2)).isEqualTo(3);
        assertThat(ProtobufWireWriter.zigZagEncode(Integer.MAX_VALUE)).isEqualTo(-2); // 0xFFFFFFFE
        assertThat(ProtobufWireWriter.zigZagEncode(Integer.MIN_VALUE)).isEqualTo(-1); // 0xFFFFFFFF
        assertThat(ProtobufWireWriter.zigZagEncode(Long.MAX_VALUE)).isEqualTo(-2L);
        assertThat(ProtobufWireWriter.zigZagEncode(Long.MIN_VALUE)).isEqualTo(-1L);

        assertThat(reader(new byte[] {0}).readSint32()).isEqualTo(0);
        assertThat(reader(new byte[] {1}).readSint32()).isEqualTo(-1);
        assertThat(reader(new byte[] {2}).readSint32()).isEqualTo(1);
        assertThat(reader(new byte[] {3}).readSint32()).isEqualTo(-2);
    }

    @Test
    public void testNegativeInt32IsSignExtended()
            throws IOException
    {
        // int32 fields encode a negative value as a ten byte varint
        ProtobufWireWriter writer = new ProtobufWireWriter();
        writer.writeInt32(1, -1);
        assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeInt32(1, -1)));
        assertThat(bytes(writer)).hasSize(1 + 10);
    }

    @Test
    public void testFixedWidthMatchesReference()
            throws IOException
    {
        for (double value : new double[] {0, -0.0, 1.5, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeDouble(8, value);
            assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeDouble(8, value)));
            ProtobufWireReader reader = reader(bytes(writer));
            assertThat(reader.readTag()).isEqualTo(makeTag(8, WIRETYPE_FIXED64));
            assertThat(Double.doubleToRawLongBits(reader.readDouble())).isEqualTo(Double.doubleToRawLongBits(value));
        }
        for (float value : new float[] {0, -0.0f, 1.5f, Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeFloat(7, value);
            assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeFloat(7, value)));
            ProtobufWireReader reader = reader(bytes(writer));
            assertThat(reader.readTag()).isEqualTo(makeTag(7, WIRETYPE_FIXED32));
            assertThat(Float.floatToRawIntBits(reader.readFloat())).isEqualTo(Float.floatToRawIntBits(value));
        }
    }

    @Test
    public void testBytesAndStringsMatchReference()
            throws IOException
    {
        for (String value : new String[] {"", "a", "zażółć gęślą jaźń", "🦃", "\0", "x".repeat(10_000)}) {
            ProtobufWireWriter writer = new ProtobufWireWriter();
            writer.writeString(10, value);
            assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeString(10, value)));
            ProtobufWireReader reader = reader(bytes(writer));
            assertThat(reader.readTag()).isEqualTo(makeTag(10, WIRETYPE_LENGTH_DELIMITED));
            assertThat(reader.readString(reader.readLength())).isEqualTo(value);
            assertThat(reader.hasRemaining()).isFalse();
        }
        byte[] bytes = new byte[300];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        ProtobufWireWriter writer = new ProtobufWireWriter(4); // forces the buffer to grow
        writer.writeBytes(12, bytes, 100, 150);
        assertThat(bytes(writer)).isEqualTo(reference(output -> output.writeBytes(12, ByteString.copyFrom(bytes, 100, 150))));
        ProtobufWireReader reader = reader(bytes(writer));
        reader.readTag();
        assertThat(reader.readBytes(reader.readLength())).containsExactly(Arrays.copyOfRange(bytes, 100, 250));
    }

    @Test
    public void testNestedMessagesMatchReference()
            throws Exception
    {
        ProtobufWireWriter row = new ProtobufWireWriter();
        ProtobufWireWriter value = row.child();
        value.writeSint64(6, -42);
        row.writeMessage(1, value);
        value = row.child();
        ProtobufWireWriter decimal = value.child();
        decimal.writeSint64(1, 12345);
        decimal.writeSint32(3, 2);
        value.writeMessage(9, decimal);
        row.writeMessage(1, value);
        value = row.child();
        value.writeEnum(1, 0);
        row.writeMessage(1, value);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ProtobufWireWriter.writeMessageTo(output, 1, row);
        ProtobufWireWriter.writeMessageTo(output, 1, new ProtobufWireWriter());

        Segment expected = Segment.newBuilder()
                .addRows(Row.newBuilder()
                        .addValues(Value.newBuilder().setBigint(-42))
                        .addValues(Value.newBuilder().setDecimal(Decimal.newBuilder().setUnscaledLong(12345).setScale(2)))
                        .addValues(Value.newBuilder().setNull(NullValue.NULL_VALUE)))
                .addRows(Row.newBuilder())
                .build();
        assertThat(Segment.parseFrom(output.toByteArray())).isEqualTo(expected);
        assertThat(output.toByteArray()).isEqualTo(expected.toByteArray());
    }

    @Test
    public void testChildWritersAreReused()
    {
        ProtobufWireWriter writer = new ProtobufWireWriter();
        ProtobufWireWriter first = writer.child();
        first.writeVarint(1);
        ProtobufWireWriter second = writer.child();
        assertThat(second).isSameAs(first);
        assertThat(second.size()).isEqualTo(0);
        assertThat(second.child()).isNotSameAs(second);
    }

    @Test
    public void testTags()
    {
        for (int field : new int[] {1, 15, 16, 2047, 2048, 536_870_911}) {
            for (int wireType : new int[] {WIRETYPE_VARINT, WIRETYPE_FIXED64, WIRETYPE_LENGTH_DELIMITED, WIRETYPE_FIXED32}) {
                ProtobufWireWriter writer = new ProtobufWireWriter();
                writer.writeTag(field, wireType);
                int tag = reader(bytes(writer)).readTag();
                assertThat(fieldNumber(tag)).isEqualTo(field);
                assertThat(wireType(tag)).isEqualTo(wireType);
            }
        }
        assertThatThrownBy(() -> reader(new byte[] {0}).readTag())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Invalid field number 0");
    }

    @Test
    public void testMalformedVarints()
            throws IOException
    {
        // continuation bit set on every byte
        byte[] endless = new byte[11];
        Arrays.fill(endless, (byte) 0x80);
        assertThatThrownBy(() -> reader(endless).readVarint64())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Malformed varint");
        // input ends in the middle of a varint
        assertThatThrownBy(() -> reader(new byte[] {(byte) 0x80}).readVarint64())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Truncated message");
        assertThatThrownBy(() -> reader(new byte[0]).readVarint64())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Truncated message");
        // a 64 bit value where 32 bits are expected
        assertThatThrownBy(() -> reader(referenceVarint(1L << 32)).readVarint32())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("does not fit in 32 bits");
        assertThatThrownBy(() -> reader(referenceVarint(-1)).readVarint32())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("does not fit in 32 bits");
        // the maximum 32 bit value is fine
        assertThat(reader(referenceVarint(0xFFFFFFFFL)).readVarint32()).isEqualTo(-1);
    }

    @Test
    public void testLengthsAreBounded()
            throws IOException
    {
        // length larger than the remaining input
        assertThatThrownBy(() -> reader(new byte[] {5, 1, 2}).readLength())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Length 5 exceeds");
        // a length that would be negative as a signed 32 bit integer
        assertThatThrownBy(() -> reader(referenceVarint(0xFFFFFFFFL)).readLength())
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("exceeds");
        // a zero length message is fine
        ProtobufWireReader reader = reader(new byte[] {0});
        int previousLimit = reader.pushLimit(reader.readLength());
        assertThat(reader.hasRemaining()).isFalse();
        reader.popLimit(previousLimit);

        assertThatThrownBy(() -> reader(new byte[] {1, 2}).readBytes(3))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Truncated message");
        assertThatThrownBy(() -> reader(new byte[] {1, 2}).readFixed32())
                .isInstanceOf(ProtobufFormatException.class);
        assertThatThrownBy(() -> reader(new byte[] {1, 2, 3, 4, 5, 6, 7}).readFixed64())
                .isInstanceOf(ProtobufFormatException.class);
    }

    @Test
    public void testNestedLimits()
    {
        // nested message of 2 bytes inside a message of 4 bytes
        ProtobufWireReader reader = reader(new byte[] {2, 7, 8, 9});
        int outerLimit = reader.pushLimit(reader.readLength());
        assertThat(reader.limit()).isEqualTo(3);
        assertThat(reader.readVarint32()).isEqualTo(7);
        // not fully consumed
        assertThatThrownBy(() -> reader.popLimit(outerLimit))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("not fully consumed, 1 bytes remaining");
        assertThat(reader.readVarint32()).isEqualTo(8);
        // reads cannot cross the nested limit
        assertThatThrownBy(reader::readVarint32)
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Truncated message");
        reader.popLimit(outerLimit);
        assertThat(reader.readVarint32()).isEqualTo(9);
        assertThat(reader.hasRemaining()).isFalse();
        // a nested message cannot be longer than its parent
        assertThatThrownBy(() -> reader(new byte[] {1}).pushLimit(2))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("exceeds the enclosing message");
    }

    @Test
    public void testSkipField()
            throws IOException
    {
        byte[] bytes = reference(output -> {
            output.writeInt64(1, Long.MIN_VALUE);
            output.writeFixed64(2, 1);
            output.writeString(3, "skipped");
            output.writeFixed32(4, 1);
            output.writeBool(5, true);
        });
        ProtobufWireReader reader = reader(bytes);
        for (int i = 0; i < 4; i++) {
            reader.skipField(reader.readTag());
        }
        assertThat(fieldNumber(reader.readTag())).isEqualTo(5);
        assertThat(reader.readBool()).isTrue();
        assertThat(reader.hasRemaining()).isFalse();

        // the deprecated group wire types are not supported
        assertThatThrownBy(() -> reader(new byte[] {0x0B}).skipField(reader(new byte[] {0x0B}).readTag()))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageContaining("Unsupported wire type 3");
    }

    @Test
    public void testReset()
    {
        ProtobufWireWriter writer = new ProtobufWireWriter();
        writer.writeString(1, "abc");
        assertThat(writer.size()).isEqualTo(5);
        writer.reset();
        assertThat(writer.size()).isEqualTo(0);
        writer.writeVarint(1);
        assertThat(bytes(writer)).containsExactly(1);

        ProtobufWireReader reader = new ProtobufWireReader();
        reader.reset(new byte[] {1, 2, 3, 4}, 1, 2);
        assertThat(reader.position()).isEqualTo(1);
        assertThat(reader.limit()).isEqualTo(3);
        assertThat(reader.readVarint32()).isEqualTo(2);
        assertThat(reader.readVarint32()).isEqualTo(3);
        assertThat(reader.hasRemaining()).isFalse();
        assertThatThrownBy(() -> reader.reset(new byte[] {1}, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testReadBigEndianSigned()
    {
        Random random = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            // at most 63 bits, which with the sign bit is at most eight bytes
            BigInteger value = new BigInteger(1 + random.nextInt(63), random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            byte[] bytes = value.toByteArray();
            assertThat(reader(bytes).readBigEndianSigned(bytes.length)).as("value %s", value).isEqualTo(value.longValueExact());
        }
        assertThat(reader(new byte[] {(byte) 0x80}).readBigEndianSigned(1)).isEqualTo(-128);
        assertThat(reader(new byte[] {0, (byte) 0xFF}).readBigEndianSigned(2)).isEqualTo(255);
        assertThat(reader(new byte[] {(byte) 0xFF, (byte) 0xFF, 0}).readBigEndianSigned(3)).isEqualTo(-256);

        ProtobufWireReader reader = reader(new byte[] {1, 2, 3});
        assertThatThrownBy(() -> reader.readBigEndianSigned(4))
                .isInstanceOf(ProtobufFormatException.class)
                .hasMessageStartingWith("Truncated message");
        assertThatThrownBy(() -> reader.readBigEndianSigned(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readBigEndianSigned(9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(reader.readBigEndianSigned(3)).isEqualTo(0x010203);
    }

    private static ProtobufWireReader reader(byte[] bytes)
    {
        ProtobufWireReader reader = new ProtobufWireReader();
        reader.reset(bytes, 0, bytes.length);
        return reader;
    }

    private static byte[] bytes(ProtobufWireWriter writer)
    {
        return Arrays.copyOf(writer.buffer(), writer.size());
    }

    private static byte[] referenceVarint(long value)
            throws IOException
    {
        return reference(output -> output.writeUInt64NoTag(value));
    }

    private interface Encoding
    {
        void encode(CodedOutputStream output)
                throws IOException;
    }

    private static byte[] reference(Encoding encoding)
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        encoding.encode(output);
        output.flush();
        // the reference implementation agrees with itself
        CodedInputStream.newInstance(bytes.toByteArray());
        return bytes.toByteArray();
    }
}
