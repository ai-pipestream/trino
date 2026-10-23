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

import com.google.protobuf.Int64Value;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorSplit;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.protobuf.handles.ProtobufHandleCodec.protobufHandleCodec;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtobufHandleCodec
{
    private record NumberedSplit(long number)
            implements ConnectorSplit {}

    private record OtherSplit()
            implements ConnectorSplit {}

    private final ConnectorHandleCodec<ConnectorSplit> codec = protobufHandleCodec(
            NumberedSplit.class,
            Int64Value.parser(),
            split -> Int64Value.of(split.number()),
            proto -> new NumberedSplit(proto.getValue()));

    @Test
    public void testRoundTrip()
    {
        for (long number : List.of(0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE)) {
            assertThat(codec.deserialize(codec.serialize(new NumberedSplit(number)))).isEqualTo(new NumberedSplit(number));
        }
        // zero is the default value, so it encodes to no bytes at all, and that must decode
        assertThat(codec.serialize(new NumberedSplit(0))).isEmpty();
    }

    @Test
    public void testWrongHandleClassIsRejected()
    {
        assertThatThrownBy(() -> codec.serialize(new OtherSplit()))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining(NumberedSplit.class.getName())
                .hasMessageContaining(OtherSplit.class.getName());
    }

    @Test
    public void testInvalidHandleIsRejected()
    {
        ConnectorHandleCodec<ConnectorSplit> strictCodec = protobufHandleCodec(
                NumberedSplit.class,
                Int64Value.parser(),
                split -> Int64Value.of(split.number()),
                proto -> {
                    if (proto.getValue() < 0) {
                        throw new IllegalArgumentException("number must not be negative");
                    }
                    return new NumberedSplit(proto.getValue());
                });
        byte[] negative = codec.serialize(new NumberedSplit(-7));
        assertThatThrownBy(() -> strictCodec.deserialize(negative))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessage("Protobuf NumberedSplit does not describe a valid handle: number must not be negative");
    }

    @Test
    public void testCorruptBytesAreRejected()
    {
        // field 1, length-delimited, declared length 5 but only one byte follows
        byte[] truncated = {0x0A, 0x05, 0x01};
        assertThatThrownBy(() -> codec.deserialize(truncated))
                .isInstanceOf(TrinoException.class)
                .satisfies(error -> assertThat(((TrinoException) error).getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining("NumberedSplit");
    }
}
