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
package io.trino.plugin.base.classloader;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleCodec;
import io.trino.spi.connector.ConnectorHandleCodecProvider;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a connector's handle codecs with the plugin class loader as the
 * thread context class loader, the way the other connector services are
 * invoked. The engine calls codecs from its own threads while encoding and
 * decoding control-plane messages.
 */
public final class ClassLoaderSafeConnectorHandleCodecProvider
        implements ConnectorHandleCodecProvider
{
    private final ConnectorHandleCodecProvider delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorHandleCodecProvider(ConnectorHandleCodecProvider delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableHandle>> getTableHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getColumnHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorSplit>> getSplitCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getSplitCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getOutputTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getInsertTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableExecuteHandle>> getTableExecuteHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getTableExecuteHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorMergeTableHandle>> getMergeTableHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getMergeTableHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorIndexHandle>> getIndexHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getIndexHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTransactionHandle>> getTransactionHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getTransactionHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorPartitioningHandle>> getPartitioningHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getPartitioningHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ExchangeSinkInstanceHandle>> getExchangeSinkInstanceHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getExchangeSinkInstanceHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ExchangeSourceHandle>> getExchangeSourceHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getExchangeSourceHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableFunctionHandle>> getTableFunctionHandleCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getTableFunctionHandleCodec);
    }

    @Override
    public Optional<ConnectorHandleCodec<ConnectorTableCredentials>> getTableCredentialsCodec()
    {
        return wrap(ConnectorHandleCodecProvider::getTableCredentialsCodec);
    }

    private <T> Optional<ConnectorHandleCodec<T>> wrap(Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> accessor)
    {
        Optional<ConnectorHandleCodec<T>> codec;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            codec = accessor.apply(delegate);
        }
        return codec.map(value -> new ClassLoaderSafeConnectorHandleCodec<>(value, classLoader));
    }
}
