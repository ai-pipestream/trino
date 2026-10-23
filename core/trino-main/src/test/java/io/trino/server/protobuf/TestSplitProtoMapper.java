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
package io.trino.server.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.connector.MockConnector.MockConnectorSplit;
import io.trino.connector.MockConnectorFactory;
import io.trino.exchange.DirectExchangeInput;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TaskId;
import io.trino.metadata.ConnectorHandleProtobufCodec;
import io.trino.metadata.Split;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchSplit;
import io.trino.server.protocol.v1.Split.KindCase;
import io.trino.spi.HostAddress;
import io.trino.split.EmptySplit;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static io.trino.connector.MockConnector.MockConnectorSplit.MOCK_CONNECTOR_SPLIT;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSplitProtoMapper
{
    private static final TaskId TASK_ID = TaskId.valueOf("query_1.2.3.1");
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("42");

    private PlanTester planTester;
    private SplitProtoMapper mapper;
    private CatalogHandle tpch;
    private CatalogHandle mock;

    @BeforeAll
    public void setUp()
    {
        planTester = PlanTester.create(testSessionBuilder().build());
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        planTester.createCatalog("mock", MockConnectorFactory.create(), ImmutableMap.of());
        mapper = new SplitProtoMapper(new ConnectorHandleProtobufCodec(planTester.getConnectorServicesProvider()));
        tpch = planTester.getCatalogHandle("tpch");
        mock = planTester.getCatalogHandle("mock");
    }

    @AfterAll
    public void tearDown()
    {
        planTester.close();
        planTester = null;
    }

    @Test
    public void testConnectorSplit()
    {
        Split split = new Split(tpch, new TpchSplit(3, 7, ImmutableList.of(HostAddress.fromParts("worker-1", 8080), HostAddress.fromParts("worker-2", 8080))));
        io.trino.server.protocol.v1.Split proto = mapper.toProto(split);
        assertThat(proto.getKindCase()).isEqualTo(KindCase.CONNECTOR_SPLIT);
        assertThat(proto.getConnectorSplit().getCatalogHandle()).isEqualTo(tpch.getId());
        assertSplitEquals(mapper.fromProto(proto), split);
    }

    @Test
    public void testConnectorSplitOfCodecLessCatalogFailsLoudly()
    {
        Split split = new Split(mock, MOCK_CONNECTOR_SPLIT);
        assertTrinoExceptionThrownBy(() -> mapper.toProto(split))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("catalog 'mock'")
                .hasMessageContaining("split handles")
                .hasMessageContaining(MockConnectorSplit.class.getName());
    }

    @Test
    public void testRemoteSplit()
    {
        Split split = new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new DirectExchangeInput(TASK_ID, "http://worker-1:8080/v1/task/query_1.2.3.1/results/0")));
        io.trino.server.protocol.v1.Split proto = mapper.toProto(split);
        assertThat(proto.getKindCase()).isEqualTo(KindCase.REMOTE_SPLIT);
        assertThat(proto.getRemoteSplit().getDirect().getTaskId()).isEqualTo("query_1.2.3.1");
        assertSplitEquals(mapper.fromProto(proto), split);
    }

    @Test
    public void testRemoteSplitMustBelongToTheRemoteCatalog()
    {
        Split split = new Split(tpch, new RemoteSplit(new DirectExchangeInput(TASK_ID, "http://worker-1:8080")));
        assertTrinoExceptionThrownBy(() -> mapper.toProto(split))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Remote split is owned by catalog");
    }

    @Test
    public void testSpoolingExchangeInputIsRejected()
    {
        Split split = new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.of(), Optional.empty())));
        assertTrinoExceptionThrownBy(() -> mapper.toProto(split))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Spooling exchange inputs");
    }

    @Test
    public void testEmptySplit()
    {
        Split split = new Split(tpch, new EmptySplit(tpch));
        io.trino.server.protocol.v1.Split proto = mapper.toProto(split);
        assertThat(proto.getKindCase()).isEqualTo(KindCase.EMPTY_SPLIT);
        assertSplitEquals(mapper.fromProto(proto), split);

        // an empty split of a catalog without codecs needs none
        assertSplitEquals(mapper.fromProto(mapper.toProto(new Split(mock, new EmptySplit(mock)))), new Split(mock, new EmptySplit(mock)));
    }

    @Test
    public void testEmptySplitCatalogMismatchIsRejected()
    {
        Split split = new Split(tpch, new EmptySplit(mock));
        assertTrinoExceptionThrownBy(() -> mapper.toProto(split))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Empty split of catalog");
    }

    @Test
    public void testSplitAssignment()
    {
        SplitAssignment assignment = new SplitAssignment(
                PLAN_NODE_ID,
                ImmutableSet.of(
                        new ScheduledSplit(0, PLAN_NODE_ID, new Split(tpch, new TpchSplit(0, 2, ImmutableList.of()))),
                        new ScheduledSplit(1, PLAN_NODE_ID, new Split(tpch, new TpchSplit(1, 2, ImmutableList.of()))),
                        new ScheduledSplit(Long.MAX_VALUE, PLAN_NODE_ID, new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new DirectExchangeInput(TASK_ID, "http://w:1"))))),
                true);
        SplitAssignment decoded = mapper.parseSplitAssignment(mapper.toByteArray(assignment));
        assertThat(decoded.getPlanNodeId()).isEqualTo(PLAN_NODE_ID);
        assertThat(decoded.isNoMoreSplits()).isTrue();
        assertThat(decoded.getSplits()).hasSize(3);
        assertThat(decoded.getSplits().stream().map(ScheduledSplit::sequenceId)).containsExactlyInAnyOrder(0L, 1L, Long.MAX_VALUE);
        for (ScheduledSplit expected : assignment.getSplits()) {
            ScheduledSplit actual = decoded.getSplits().stream().filter(split -> split.sequenceId() == expected.sequenceId()).findFirst().orElseThrow();
            assertThat(actual.planNodeId()).isEqualTo(expected.planNodeId());
            assertSplitEquals(actual.split(), expected.split());
        }
    }

    @Test
    public void testEmptySplitAssignment()
    {
        SplitAssignment assignment = new SplitAssignment(PLAN_NODE_ID, ImmutableSet.of(), false);
        SplitAssignment decoded = mapper.parseSplitAssignment(mapper.toByteArray(assignment));
        assertThat(decoded.getPlanNodeId()).isEqualTo(PLAN_NODE_ID);
        assertThat(decoded.getSplits()).isEmpty();
        assertThat(decoded.isNoMoreSplits()).isFalse();
    }

    @Test
    public void testCorruptBytesAreRejected()
    {
        assertTrinoExceptionThrownBy(() -> mapper.parseSplitAssignment(new byte[] {(byte) 0xFF, (byte) 0xFF}))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("Failed to decode protobuf SplitAssignment");
    }

    @Test
    public void testIncompleteMessagesAreRejected()
    {
        assertTrinoExceptionThrownBy(() -> mapper.fromProto(io.trino.server.protocol.v1.Split.getDefaultInstance()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no kind");
        assertTrinoExceptionThrownBy(() -> mapper.fromProto(io.trino.server.protocol.v1.ScheduledSplit.newBuilder().setSequenceId(5).build()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("scheduled split 5 has no split");
        assertTrinoExceptionThrownBy(() -> mapper.fromProto(io.trino.server.protocol.v1.Split.newBuilder()
                .setRemoteSplit(io.trino.server.protocol.v1.RemoteSplit.getDefaultInstance())
                .build()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("no exchange input");
        assertTrinoExceptionThrownBy(() -> mapper.fromProto(io.trino.server.protocol.v1.Split.newBuilder()
                .setRemoteSplit(io.trino.server.protocol.v1.RemoteSplit.newBuilder()
                        .setDirect(io.trino.server.protocol.v1.DirectExchangeInput.newBuilder().setTaskId("nonsense").setLocation("http://w:1")))
                .build()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid task id: nonsense");
        assertTrinoExceptionThrownBy(() -> mapper.fromProto(io.trino.server.protocol.v1.Split.newBuilder()
                .setEmptySplit(io.trino.server.protocol.v1.EmptySplit.newBuilder().setCatalogHandle("nonsense"))
                .build()))
                .hasErrorCode(GENERIC_INTERNAL_ERROR)
                .hasMessageContaining("invalid catalog handle id: nonsense");
    }

    private static void assertSplitEquals(Split actual, Split expected)
    {
        assertThat(actual.getCatalogHandle()).isEqualTo(expected.getCatalogHandle());
        assertThat(actual.getConnectorSplit().getClass()).isEqualTo(expected.getConnectorSplit().getClass());
        switch (expected.getConnectorSplit()) {
            case RemoteSplit remote -> {
                DirectExchangeInput expectedInput = (DirectExchangeInput) remote.getExchangeInput();
                DirectExchangeInput actualInput = (DirectExchangeInput) ((RemoteSplit) actual.getConnectorSplit()).getExchangeInput();
                assertThat(actualInput.getTaskId()).isEqualTo(expectedInput.getTaskId());
                assertThat(actualInput.getLocation()).isEqualTo(expectedInput.getLocation());
            }
            case EmptySplit empty -> assertThat(((EmptySplit) actual.getConnectorSplit()).getCatalogHandle()).isEqualTo(empty.getCatalogHandle());
            default -> assertThat(actual.getConnectorSplit()).isEqualTo(expected.getConnectorSplit());
        }
    }
}
