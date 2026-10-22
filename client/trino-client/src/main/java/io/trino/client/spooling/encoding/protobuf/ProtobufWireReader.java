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

import java.util.Arrays;

import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED32;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED64;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_VARINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.fieldNumber;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.wireType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads protobuf wire format from a byte array. A nested message is read by narrowing the
 * reader with {@link #pushLimit(int)} and restoring it with {@link #popLimit(int)} afterwards.
 * <p>
 * Instances are not thread-safe.
 */
public final class ProtobufWireReader
{
    private byte[] buffer = new byte[0];
    private int position;
    private int limit;

    public void reset(byte[] buffer, int offset, int length)
    {
        if (offset < 0 || length < 0 || offset + length > buffer.length) {
            throw new IllegalArgumentException(format("Invalid range [%s, %s) for buffer of length %s", offset, offset + length, buffer.length));
        }
        this.buffer = buffer;
        this.position = offset;
        this.limit = offset + length;
    }

    public int position()
    {
        return position;
    }

    public int limit()
    {
        return limit;
    }

    public boolean hasRemaining()
    {
        return position < limit;
    }

    /**
     * Narrows the reader to the next {@code length} bytes and returns the previous limit.
     */
    public int pushLimit(int length)
    {
        if (length < 0 || length > limit - position) {
            throw new ProtobufFormatException(format("Message of length %s exceeds the enclosing message at position %s, limit %s", length, position, limit));
        }
        int previousLimit = limit;
        limit = position + length;
        return previousLimit;
    }

    /**
     * Restores the limit saved by {@link #pushLimit(int)}, checking that the nested message was fully consumed.
     */
    public void popLimit(int previousLimit)
    {
        if (position != limit) {
            throw new ProtobufFormatException(format("Message was not fully consumed, %s bytes remaining", limit - position));
        }
        limit = previousLimit;
    }

    /**
     * Reads a field tag: {@code (fieldNumber << 3) | wireType}.
     */
    public int readTag()
    {
        int tag = readVarint32();
        if (fieldNumber(tag) == 0) {
            throw new ProtobufFormatException("Invalid field number 0 at position " + position);
        }
        return tag;
    }

    public int readVarint32()
    {
        long value = readVarint64();
        if (value != (value & 0xFFFFFFFFL)) {
            throw new ProtobufFormatException("Varint does not fit in 32 bits: " + value);
        }
        return (int) value;
    }

    public long readVarint64()
    {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new ProtobufFormatException("Malformed varint at position " + position);
    }

    public int readSint32()
    {
        int value = readVarint32();
        return (value >>> 1) ^ -(value & 1);
    }

    public long readSint64()
    {
        long value = readVarint64();
        return (value >>> 1) ^ -(value & 1);
    }

    public boolean readBool()
    {
        return readVarint64() != 0;
    }

    public int readFixed32()
    {
        ensureRemaining(4);
        int value = (buffer[position] & 0xFF)
                | (buffer[position + 1] & 0xFF) << 8
                | (buffer[position + 2] & 0xFF) << 16
                | (buffer[position + 3] & 0xFF) << 24;
        position += 4;
        return value;
    }

    public long readFixed64()
    {
        ensureRemaining(8);
        long value = 0;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (buffer[position + i] & 0xFF);
        }
        position += 8;
        return value;
    }

    public float readFloat()
    {
        return Float.intBitsToFloat(readFixed32());
    }

    public double readDouble()
    {
        return Double.longBitsToDouble(readFixed64());
    }

    /**
     * Reads the length prefix of a length-delimited field.
     */
    public int readLength()
    {
        int length = readVarint32();
        if (length < 0 || length > limit - position) {
            throw new ProtobufFormatException(format("Length %s exceeds the enclosing message at position %s, limit %s", length, position, limit));
        }
        return length;
    }

    public byte[] readBytes(int length)
    {
        ensureRemaining(length);
        byte[] result = Arrays.copyOfRange(buffer, position, position + length);
        position += length;
        return result;
    }

    public String readString(int length)
    {
        ensureRemaining(length);
        String result = new String(buffer, position, length, UTF_8);
        position += length;
        return result;
    }

    public void skip(int length)
    {
        ensureRemaining(length);
        position += length;
    }

    public void skipField(int tag)
    {
        switch (wireType(tag)) {
            case WIRETYPE_VARINT:
                readVarint64();
                break;
            case WIRETYPE_FIXED64:
                skip(8);
                break;
            case WIRETYPE_LENGTH_DELIMITED:
                skip(readLength());
                break;
            case WIRETYPE_FIXED32:
                skip(4);
                break;
            default:
                throw new ProtobufFormatException(format("Unsupported wire type %s for field %s", wireType(tag), fieldNumber(tag)));
        }
    }

    private byte readByte()
    {
        ensureRemaining(1);
        return buffer[position++];
    }

    private void ensureRemaining(int length)
    {
        if (length < 0 || length > limit - position) {
            throw new ProtobufFormatException(format("Truncated message: needed %s bytes at position %s, limit %s", length, position, limit));
        }
    }
}
