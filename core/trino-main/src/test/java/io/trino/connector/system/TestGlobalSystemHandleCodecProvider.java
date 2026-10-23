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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import io.trino.jsonpath.ir.IrJsonPath;
import io.trino.operator.table.ExcludeColumnsFunction.ExcludeColumnsFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionHandle;
import io.trino.operator.table.SequenceFunction.SequenceFunctionSplit;
import io.trino.operator.table.json.JsonTable.JsonTableFunctionHandle;
import io.trino.operator.table.json.JsonTablePlanLeaf;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.Type;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.planner.PathNodes.contextVariable;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGlobalSystemHandleCodecProvider
{
    private final GlobalSystemHandleCodecProvider provider = new GlobalSystemHandleCodecProvider();

    @Test
    public void testTransactionHandle()
    {
        ConnectorHandleCodec<ConnectorTransactionHandle> codec = provider.getTransactionHandleCodec().orElseThrow();
        GlobalSystemTransactionHandle handle = new GlobalSystemTransactionHandle(TransactionId.create());
        assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);
    }

    @Test
    public void testTableFunctionHandles()
    {
        ConnectorHandleCodec<ConnectorTableFunctionHandle> codec = provider.getTableFunctionHandleCodec().orElseThrow();
        assertThat(codec.deserialize(codec.serialize(new ExcludeColumnsFunctionHandle()))).isEqualTo(new ExcludeColumnsFunctionHandle());
        for (SequenceFunctionHandle handle : ImmutableList.of(
                new SequenceFunctionHandle(0, 0, 1),
                new SequenceFunctionHandle(Long.MIN_VALUE, Long.MAX_VALUE, 1),
                new SequenceFunctionHandle(10, -10, -3))) {
            assertThat(codec.deserialize(codec.serialize(handle))).isEqualTo(handle);
        }
    }

    @Test
    public void testJsonTableHandleIsRejected()
    {
        ConnectorHandleCodec<ConnectorTableFunctionHandle> codec = provider.getTableFunctionHandleCodec().orElseThrow();
        JsonTableFunctionHandle handle = new JsonTableFunctionHandle(
                new JsonTablePlanLeaf(new IrJsonPath(true, contextVariable()), ImmutableList.of()),
                false,
                true,
                anonymousRow(BIGINT),
                new Type[] {BIGINT});
        assertTrinoExceptionThrownBy(() -> codec.serialize(handle))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("json_table");
    }

    @Test
    public void testFunctionNotSetIsRejected()
    {
        ConnectorHandleCodec<ConnectorTableFunctionHandle> codec = provider.getTableFunctionHandleCodec().orElseThrow();
        assertTrinoExceptionThrownBy(() -> codec.deserialize(new byte[0]))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no function");
    }

    @Test
    public void testSplit()
    {
        ConnectorHandleCodec<ConnectorSplit> codec = provider.getSplitCodec().orElseThrow();
        SequenceFunctionSplit split = new SequenceFunctionSplit(Long.MIN_VALUE, Long.MAX_VALUE);
        SequenceFunctionSplit decoded = (SequenceFunctionSplit) codec.deserialize(codec.serialize(split));
        assertThat(decoded.getStart()).isEqualTo(Long.MIN_VALUE);
        assertThat(decoded.getStop()).isEqualTo(Long.MAX_VALUE);

        // all-default values encode to zero bytes and still decode
        SequenceFunctionSplit zero = (SequenceFunctionSplit) codec.deserialize(codec.serialize(new SequenceFunctionSplit(0, 0)));
        assertThat(zero.getStart()).isEqualTo(0);
        assertThat(zero.getStop()).isEqualTo(0);
    }
}
