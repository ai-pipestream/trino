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
package io.trino.protobuf.handles;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.BitArrayBlockEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Fixed12BlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.block.VariantBlockEncoding;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Minimal block serde over the SPI block encodings, for codec tests. The
 * engine's serde lives in trino-main, which depends on this module, so tests
 * of this module and of connectors the engine depends on use this one.
 */
public final class TestingBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final Map<String, BlockEncoding> encodings = new HashMap<>();
    private final Map<Class<? extends Block>, BlockEncoding> encodingsByBlockClass = new HashMap<>();
    private final TypeManager typeManager;

    public TestingBlockEncodingSerde(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        for (BlockEncoding encoding : new BlockEncoding[] {
                new VariableWidthBlockEncoding(),
                new BitArrayBlockEncoding(),
                new ByteArrayBlockEncoding(false, false),
                new ShortArrayBlockEncoding(false, false),
                new IntArrayBlockEncoding(false, false),
                new LongArrayBlockEncoding(false, false),
                new Fixed12BlockEncoding(),
                new Int128ArrayBlockEncoding(),
                new VariantBlockEncoding(),
                new DictionaryBlockEncoding(),
                new ArrayBlockEncoding(),
                new MapBlockEncoding(),
                new RowBlockEncoding(),
                new RunLengthBlockEncoding(),
        }) {
            encodings.put(encoding.getName(), encoding);
            encodingsByBlockClass.put(encoding.getBlockClass(), encoding);
        }
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        return encoding(readString(input)).readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
        while (true) {
            BlockEncoding encoding = encodingsByBlockClass.get(block.getClass());
            if (encoding == null) {
                throw new IllegalArgumentException("Unknown block class " + block.getClass().getName());
            }
            Optional<Block> replacement = encoding.replacementBlockForWrite(block);
            if (replacement.isEmpty()) {
                writeString(output, encoding.getName());
                encoding.writeBlock(this, output, block);
                return;
            }
            block = replacement.get();
        }
    }

    @Override
    public Type readType(SliceInput input)
    {
        return typeManager.getType(TypeId.of(readString(input)));
    }

    @Override
    public void writeType(SliceOutput output, Type type)
    {
        writeString(output, type.getTypeId().getId());
    }

    private BlockEncoding encoding(String name)
    {
        BlockEncoding encoding = encodings.get(name);
        if (encoding == null) {
            throw new IllegalArgumentException("Unknown block encoding: " + name);
        }
        return encoding;
    }

    private static String readString(SliceInput input)
    {
        byte[] bytes = new byte[input.readInt()];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private static void writeString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
