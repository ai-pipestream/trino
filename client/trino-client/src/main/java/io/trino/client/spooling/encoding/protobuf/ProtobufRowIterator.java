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

import io.trino.client.CloseableIterator;
import io.trino.client.spooling.encoding.protobuf.ProtobufValueDecoders.ValueDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.ROW_VALUES;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.SEGMENT_ROWS;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED32;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_FIXED64;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.WIRETYPE_VARINT;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.fieldNumber;
import static io.trino.client.spooling.encoding.protobuf.ProtobufWireFormat.wireType;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Streams the rows of one {@code Segment} message: each {@code rows} entry is read into a reusable
 * buffer and decoded, so only one row is held in memory beyond the input stream's own buffering.
 */
public class ProtobufRowIterator
        implements CloseableIterator<List<Object>>
{
    private static final int MAX_ROW_SIZE = Integer.MAX_VALUE - 8;
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;

    private final InputStream input;
    private final ValueDecoder[] decoders;
    private final ProtobufWireReader reader = new ProtobufWireReader();
    // The segment is read in chunks, and each row is decoded in place from [position, limit)
    private byte[] buffer = new byte[INITIAL_BUFFER_SIZE];
    private int position;
    private int limit;
    private boolean endOfInput;
    private List<Object> nextRow;
    private boolean finished;

    public ProtobufRowIterator(InputStream input, ValueDecoder[] decoders)
    {
        this.input = requireNonNull(input, "input is null");
        this.decoders = requireNonNull(decoders, "decoders is null");
    }

    @Override
    public boolean hasNext()
    {
        if (nextRow != null) {
            return true;
        }
        if (finished) {
            return false;
        }
        try {
            nextRow = readRow();
        }
        catch (IOException e) {
            finish();
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException e) {
            finish();
            throw e;
        }
        if (nextRow == null) {
            finish();
            return false;
        }
        return true;
    }

    @Override
    public List<Object> next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        List<Object> row = nextRow;
        nextRow = null;
        return row;
    }

    private List<Object> readRow()
            throws IOException
    {
        while (true) {
            if (!ensureAvailable(1)) {
                // a clean end of input
                return null;
            }
            long value = readVarint();
            if (value > 0xFFFFFFFFL || fieldNumber((int) value) == 0) {
                throw new ProtobufFormatException("Invalid segment field tag: " + value);
            }
            int tag = (int) value;
            if (fieldNumber(tag) != SEGMENT_ROWS) {
                // a field added to Segment by a newer server
                skipField(tag);
                continue;
            }
            if (wireType(tag) != WIRETYPE_LENGTH_DELIMITED) {
                throw new ProtobufFormatException(format("Unexpected wire type %s for segment rows", wireType(tag)));
            }
            long length = readVarint();
            if (length > MAX_ROW_SIZE) {
                throw new ProtobufFormatException("Invalid row length: " + length);
            }
            int rowLength = (int) length;
            if (!ensureAvailable(rowLength)) {
                throw new EOFException(format("Truncated row: expected %s bytes but got %s", rowLength, limit - position));
            }
            reader.reset(buffer, position, rowLength);
            position += rowLength;
            return decodeRow();
        }
    }

    /**
     * Reads from the input until at least {@code length} unread bytes are buffered, or the input ends.
     * The buffer grows with the bytes actually read, so a corrupt length cannot make the client allocate
     * more memory than the segment holds.
     */
    private boolean ensureAvailable(int length)
            throws IOException
    {
        while (limit - position < length) {
            if (endOfInput) {
                return false;
            }
            if (position > 0 && (position == limit || buffer.length - position < length)) {
                System.arraycopy(buffer, position, buffer, 0, limit - position);
                limit -= position;
                position = 0;
            }
            if (limit == buffer.length) {
                buffer = Arrays.copyOf(buffer, (int) Math.min(length, 2L * buffer.length));
            }
            int read = input.read(buffer, limit, buffer.length - limit);
            if (read == -1) {
                endOfInput = true;
            }
            else {
                limit += read;
            }
        }
        return true;
    }

    private List<Object> decodeRow()
    {
        List<Object> row = new ArrayList<>(decoders.length);
        while (reader.hasRemaining()) {
            int tag = reader.readTag();
            if (fieldNumber(tag) != ROW_VALUES) {
                reader.skipField(tag);
                continue;
            }
            if (wireType(tag) != WIRETYPE_LENGTH_DELIMITED) {
                throw new ProtobufFormatException("Unexpected wire type for row values: " + wireType(tag));
            }
            if (row.size() >= decoders.length) {
                throw new ProtobufFormatException(format("Row has more than %s values", decoders.length));
            }
            row.add(ProtobufValueDecoders.readValue(reader, decoders[row.size()]));
        }
        if (row.size() != decoders.length) {
            throw new ProtobufFormatException(format("Row has %s values but %s columns were expected", row.size(), decoders.length));
        }
        return unmodifiableList(row);
    }

    /**
     * Reads a varint of the segment itself. The caller has checked that at least one byte is available.
     */
    private long readVarint()
            throws IOException
    {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (!ensureAvailable(1)) {
                throw new EOFException("Truncated varint");
            }
            byte b = buffer[position++];
            result |= (long) (b & 0x7F) << shift;
            if (b >= 0) {
                if (result < 0) {
                    throw new ProtobufFormatException("Varint out of range: " + result);
                }
                return result;
            }
        }
        throw new ProtobufFormatException("Malformed varint");
    }

    private void skipField(int tag)
            throws IOException
    {
        switch (wireType(tag)) {
            case WIRETYPE_VARINT:
                readVarint();
                break;
            case WIRETYPE_FIXED64:
                skip(8);
                break;
            case WIRETYPE_LENGTH_DELIMITED:
                long length = readVarint();
                if (length > MAX_ROW_SIZE) {
                    throw new ProtobufFormatException("Invalid field length: " + length);
                }
                skip((int) length);
                break;
            case WIRETYPE_FIXED32:
                skip(4);
                break;
            default:
                throw new ProtobufFormatException(format("Unsupported wire type %s for field %s", wireType(tag), fieldNumber(tag)));
        }
    }

    private void skip(int length)
            throws IOException
    {
        int remaining = length;
        while (remaining > 0) {
            if (position == limit && !ensureAvailable(1)) {
                throw new EOFException("Truncated segment");
            }
            int chunk = Math.min(remaining, limit - position);
            position += chunk;
            remaining -= chunk;
        }
    }

    private void finish()
    {
        finished = true;
        nextRow = null;
        try {
            input.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
    {
        if (!finished) {
            finish();
        }
    }

    @Override
    public String toString()
    {
        return "ProtobufRowIterator{columns=" + decoders.length + "}";
    }
}
