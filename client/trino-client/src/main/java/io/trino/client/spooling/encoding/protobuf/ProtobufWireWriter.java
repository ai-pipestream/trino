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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED32;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED64;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_VARINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.makeTag;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Writes protobuf wire format into a growable, reusable buffer.
 * <p>
 * The content of a nested message is written into a {@link #child()} writer first and then
 * copied into this writer with {@link #writeMessage(int, ProtobufWireWriter)}, which is how the
 * length prefix gets known. Each writer keeps one child, so a chain of writers is as deep as the
 * deepest nested message and is reused for every value.
 * <p>
 * Instances are not thread-safe.
 */
public final class ProtobufWireWriter
{
    private static final int MAX_VARINT_SIZE = 10;

    private byte[] buffer;
    private int size;
    private ProtobufWireWriter child;

    public ProtobufWireWriter()
    {
        this(256);
    }

    public ProtobufWireWriter(int initialCapacity)
    {
        this.buffer = new byte[initialCapacity];
    }

    /**
     * Returns an empty writer for the content of a nested message.
     */
    public ProtobufWireWriter child()
    {
        if (child == null) {
            child = new ProtobufWireWriter(Math.max(64, buffer.length / 4));
        }
        child.reset();
        return child;
    }

    public void reset()
    {
        size = 0;
    }

    public int size()
    {
        return size;
    }

    /**
     * Backing array; only the first {@link #size()} bytes are valid, and only until the next write.
     */
    public byte[] buffer()
    {
        return buffer;
    }

    public void writeTo(OutputStream output)
            throws IOException
    {
        output.write(buffer, 0, size);
    }

    /**
     * Writes a length-delimited field directly to the output stream, without copying the message.
     */
    public static void writeMessageTo(OutputStream output, int fieldNumber, ProtobufWireWriter message)
            throws IOException
    {
        byte[] header = new byte[2 * MAX_VARINT_SIZE];
        int headerSize = putVarint(header, 0, makeTag(fieldNumber, WIRETYPE_LENGTH_DELIMITED));
        headerSize = putVarint(header, headerSize, message.size);
        output.write(header, 0, headerSize);
        message.writeTo(output);
    }

    public void writeTag(int fieldNumber, int wireType)
    {
        // tags are unsigned 32 bit varints: do not sign extend the largest field numbers
        writeVarint(makeTag(fieldNumber, wireType) & 0xFFFFFFFFL);
    }

    public void writeVarint(long value)
    {
        ensureCapacity(MAX_VARINT_SIZE);
        size = putVarint(buffer, size, value);
    }

    public void writeBool(int fieldNumber, boolean value)
    {
        ensureCapacity(MAX_VARINT_SIZE + 1);
        size = putVarint(buffer, size, makeTag(fieldNumber, WIRETYPE_VARINT));
        buffer[size++] = (byte) (value ? 1 : 0);
    }

    /**
     * Writes an {@code int32}, {@code int64} or {@code enum} field: plain varint, sign-extended if negative.
     */
    public void writeInt64(int fieldNumber, long value)
    {
        ensureCapacity(2 * MAX_VARINT_SIZE);
        size = putVarint(buffer, size, makeTag(fieldNumber, WIRETYPE_VARINT));
        size = putVarint(buffer, size, value);
    }

    public void writeInt32(int fieldNumber, int value)
    {
        writeInt64(fieldNumber, value);
    }

    public void writeEnum(int fieldNumber, int value)
    {
        writeInt64(fieldNumber, value);
    }

    public void writeSint32(int fieldNumber, int value)
    {
        writeInt64(fieldNumber, zigZagEncode(value) & 0xFFFFFFFFL);
    }

    public void writeSint64(int fieldNumber, long value)
    {
        writeInt64(fieldNumber, zigZagEncode(value));
    }

    public void writeFloat(int fieldNumber, float value)
    {
        writeFixed32(fieldNumber, Float.floatToRawIntBits(value));
    }

    public void writeDouble(int fieldNumber, double value)
    {
        writeFixed64(fieldNumber, Double.doubleToRawLongBits(value));
    }

    public void writeFixed32(int fieldNumber, int value)
    {
        ensureCapacity(MAX_VARINT_SIZE + 4);
        size = putVarint(buffer, size, makeTag(fieldNumber, WIRETYPE_FIXED32));
        buffer[size++] = (byte) value;
        buffer[size++] = (byte) (value >>> 8);
        buffer[size++] = (byte) (value >>> 16);
        buffer[size++] = (byte) (value >>> 24);
    }

    public void writeFixed64(int fieldNumber, long value)
    {
        ensureCapacity(MAX_VARINT_SIZE + 8);
        size = putVarint(buffer, size, makeTag(fieldNumber, WIRETYPE_FIXED64));
        for (int shift = 0; shift < 64; shift += 8) {
            buffer[size++] = (byte) (value >>> shift);
        }
    }

    public void writeBytes(int fieldNumber, byte[] value)
    {
        writeBytes(fieldNumber, value, 0, value.length);
    }

    public void writeBytes(int fieldNumber, byte[] value, int offset, int length)
    {
        ensureCapacity(2 * MAX_VARINT_SIZE + length);
        size = putVarint(buffer, size, makeTag(fieldNumber, WIRETYPE_LENGTH_DELIMITED));
        size = putVarint(buffer, size, length);
        System.arraycopy(value, offset, buffer, size, length);
        size += length;
    }

    public void writeString(int fieldNumber, String value)
    {
        writeBytes(fieldNumber, requireNonNull(value, "value is null").getBytes(UTF_8));
    }

    public void writeMessage(int fieldNumber, ProtobufWireWriter message)
    {
        writeBytes(fieldNumber, message.buffer, 0, message.size);
    }

    private void ensureCapacity(int additional)
    {
        int required = size + additional;
        if (required > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(required, buffer.length * 2));
        }
    }

    private static int putVarint(byte[] buffer, int position, long value)
    {
        while ((value & ~0x7FL) != 0) {
            buffer[position++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[position++] = (byte) value;
        return position;
    }

    static int zigZagEncode(int value)
    {
        return (value << 1) ^ (value >> 31);
    }

    static long zigZagEncode(long value)
    {
        return (value << 1) ^ (value >> 63);
    }
}
