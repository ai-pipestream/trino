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
package io.trino.spi.connector;

import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

/**
 * Provides binary codecs for the connector handle families this connector
 * implements: one accessor per family the engine otherwise serializes with
 * Jackson. A connector that returns no provider, or no codec for a family in
 * use, fails queries loudly when the control plane encoding is protobuf;
 * there is no JSON fallback.
 */
public interface ConnectorHandleCodecProvider
{
    default Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorTableExecuteHandle>> getTableExecuteHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorMergeTableHandle>> getMergeTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorIndexHandle>> getIndexHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorPartitioningHandle>> getPartitioningHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ExchangeSinkInstanceHandle>> getExchangeSinkInstanceHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ExchangeSourceHandle>> getExchangeSourceHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorTableFunctionHandle>> getTableFunctionHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorHandleCodec<ConnectorTableCredentials>> getTableCredentialsCodec()
    {
        return Optional.empty();
    }
}
