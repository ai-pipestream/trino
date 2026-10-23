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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import io.trino.connector.CatalogHandle;
import io.trino.exchange.DirectExchangeInput;
import io.trino.exchange.ExchangeInput;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.metadata.ConnectorHandleProtobufCodec;
import io.trino.metadata.Split;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.split.EmptySplit;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import static io.trino.metadata.HandleFamily.SPLIT;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.server.protobuf.ExecutionFailureInfoProtoMapper.newCodedInputStream;
import static io.trino.server.protobuf.ProtoIdentifiers.parseCatalogHandle;
import static io.trino.server.protobuf.ProtoIdentifiers.parseTaskId;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Protobuf form of {@link Split}, {@link ScheduledSplit} and
 * {@link SplitAssignment}. Remote and empty splits are engine-owned and
 * mapped here; every other split is encoded by its catalog's handle codec.
 */
public class SplitProtoMapper
{
    private final ConnectorHandleProtobufCodec handleCodec;

    @Inject
    public SplitProtoMapper(ConnectorHandleProtobufCodec handleCodec)
    {
        this.handleCodec = requireNonNull(handleCodec, "handleCodec is null");
    }

    public io.trino.server.protocol.v1.Split toProto(Split split)
    {
        io.trino.server.protocol.v1.Split.Builder builder = io.trino.server.protocol.v1.Split.newBuilder();
        CatalogHandle catalogHandle = split.getCatalogHandle();
        switch (split.getConnectorSplit()) {
            case RemoteSplit remoteSplit -> {
                if (!catalogHandle.equals(REMOTE_CATALOG_HANDLE)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Remote split is owned by catalog %s instead of %s", catalogHandle, REMOTE_CATALOG_HANDLE));
                }
                builder.setRemoteSplit(toProto(remoteSplit));
            }
            case EmptySplit emptySplit -> {
                if (!catalogHandle.equals(emptySplit.getCatalogHandle())) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Empty split of catalog %s is owned by catalog %s", emptySplit.getCatalogHandle(), catalogHandle));
                }
                builder.setEmptySplit(io.trino.server.protocol.v1.EmptySplit.newBuilder()
                        .setCatalogHandle(catalogHandle.getId()));
            }
            case ConnectorSplit connectorSplit -> builder.setConnectorSplit(handleCodec.encode(catalogHandle, SPLIT, connectorSplit));
        }
        return builder.build();
    }

    public Split fromProto(io.trino.server.protocol.v1.Split proto)
    {
        return switch (proto.getKindCase()) {
            case CONNECTOR_SPLIT -> new Split(
                    parseCatalogHandle(proto.getConnectorSplit().getCatalogHandle()),
                    handleCodec.decode(SPLIT, proto.getConnectorSplit()));
            case REMOTE_SPLIT -> new Split(REMOTE_CATALOG_HANDLE, fromProto(proto.getRemoteSplit()));
            case EMPTY_SPLIT -> {
                CatalogHandle catalogHandle = parseCatalogHandle(proto.getEmptySplit().getCatalogHandle());
                yield new Split(catalogHandle, new EmptySplit(catalogHandle));
            }
            case KIND_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf split has no kind");
        };
    }

    public io.trino.server.protocol.v1.ScheduledSplit toProto(ScheduledSplit split)
    {
        return io.trino.server.protocol.v1.ScheduledSplit.newBuilder()
                .setSequenceId(split.sequenceId())
                .setPlanNodeId(split.planNodeId().toString())
                .setSplit(toProto(split.split()))
                .build();
    }

    public ScheduledSplit fromProto(io.trino.server.protocol.v1.ScheduledSplit proto)
    {
        if (!proto.hasSplit()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf scheduled split " + proto.getSequenceId() + " has no split");
        }
        return new ScheduledSplit(proto.getSequenceId(), new PlanNodeId(proto.getPlanNodeId()), fromProto(proto.getSplit()));
    }

    public io.trino.server.protocol.v1.SplitAssignment toProto(SplitAssignment assignment)
    {
        io.trino.server.protocol.v1.SplitAssignment.Builder builder = io.trino.server.protocol.v1.SplitAssignment.newBuilder()
                .setPlanNodeId(assignment.getPlanNodeId().toString())
                .setNoMoreSplits(assignment.isNoMoreSplits());
        for (ScheduledSplit split : assignment.getSplits()) {
            builder.addSplits(toProto(split));
        }
        return builder.build();
    }

    public SplitAssignment fromProto(io.trino.server.protocol.v1.SplitAssignment proto)
    {
        ImmutableSet.Builder<ScheduledSplit> splits = ImmutableSet.builderWithExpectedSize(proto.getSplitsCount());
        for (io.trino.server.protocol.v1.ScheduledSplit split : proto.getSplitsList()) {
            splits.add(fromProto(split));
        }
        return new SplitAssignment(new PlanNodeId(proto.getPlanNodeId()), splits.build(), proto.getNoMoreSplits());
    }

    public byte[] toByteArray(SplitAssignment assignment)
    {
        return toProto(assignment).toByteArray();
    }

    public SplitAssignment parseSplitAssignment(byte[] bytes)
    {
        try {
            return fromProto(io.trino.server.protocol.v1.SplitAssignment.parser().parseFrom(newCodedInputStream(bytes)));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf SplitAssignment", e);
        }
    }

    private static io.trino.server.protocol.v1.RemoteSplit toProto(RemoteSplit split)
    {
        return switch (split.getExchangeInput()) {
            case DirectExchangeInput input -> io.trino.server.protocol.v1.RemoteSplit.newBuilder()
                    .setDirect(io.trino.server.protocol.v1.DirectExchangeInput.newBuilder()
                            .setTaskId(input.getTaskId().toString())
                            .setLocation(input.getLocation()))
                    .build();
            case SpoolingExchangeInput _ -> throw new TrinoException(NOT_SUPPORTED, "Spooling exchange inputs have no protobuf encoding yet; " +
                    "fault-tolerant execution requires the JSON control plane");
            case ExchangeInput input -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown exchange input type " + input.getClass().getName());
        };
    }

    private static RemoteSplit fromProto(io.trino.server.protocol.v1.RemoteSplit proto)
    {
        return switch (proto.getExchangeInputCase()) {
            case DIRECT -> {
                io.trino.server.protocol.v1.DirectExchangeInput direct = proto.getDirect();
                yield new RemoteSplit(new DirectExchangeInput(parseTaskId(direct.getTaskId()), direct.getLocation()));
            }
            case EXCHANGEINPUT_NOT_SET -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf remote split has no exchange input");
        };
    }
}
