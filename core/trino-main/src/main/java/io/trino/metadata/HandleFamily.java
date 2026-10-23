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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * One connector handle family the engine serializes on the control plane,
 * paired with the {@link ConnectorHandleCodecProvider} accessor that yields
 * its codec. The family name appears in error messages when a connector
 * provides no codec.
 */
public final class HandleFamily<T>
{
    public static final HandleFamily<ConnectorTableHandle> TABLE = new HandleFamily<>("table", ConnectorTableHandle.class, ConnectorHandleCodecProvider::getTableHandleCodec);
    public static final HandleFamily<ColumnHandle> COLUMN = new HandleFamily<>("column", ColumnHandle.class, ConnectorHandleCodecProvider::getColumnHandleCodec);
    public static final HandleFamily<ConnectorSplit> SPLIT = new HandleFamily<>("split", ConnectorSplit.class, ConnectorHandleCodecProvider::getSplitCodec);
    public static final HandleFamily<ConnectorOutputTableHandle> OUTPUT_TABLE = new HandleFamily<>("output table", ConnectorOutputTableHandle.class, ConnectorHandleCodecProvider::getOutputTableHandleCodec);
    public static final HandleFamily<ConnectorInsertTableHandle> INSERT_TABLE = new HandleFamily<>("insert table", ConnectorInsertTableHandle.class, ConnectorHandleCodecProvider::getInsertTableHandleCodec);
    public static final HandleFamily<ConnectorTableExecuteHandle> TABLE_EXECUTE = new HandleFamily<>("table execute", ConnectorTableExecuteHandle.class, ConnectorHandleCodecProvider::getTableExecuteHandleCodec);
    public static final HandleFamily<ConnectorMergeTableHandle> MERGE_TABLE = new HandleFamily<>("merge table", ConnectorMergeTableHandle.class, ConnectorHandleCodecProvider::getMergeTableHandleCodec);
    public static final HandleFamily<ConnectorIndexHandle> INDEX = new HandleFamily<>("index", ConnectorIndexHandle.class, ConnectorHandleCodecProvider::getIndexHandleCodec);
    public static final HandleFamily<ConnectorTransactionHandle> TRANSACTION = new HandleFamily<>("transaction", ConnectorTransactionHandle.class, ConnectorHandleCodecProvider::getTransactionHandleCodec);
    public static final HandleFamily<ConnectorPartitioningHandle> PARTITIONING = new HandleFamily<>("partitioning", ConnectorPartitioningHandle.class, ConnectorHandleCodecProvider::getPartitioningHandleCodec);
    public static final HandleFamily<ExchangeSinkInstanceHandle> EXCHANGE_SINK_INSTANCE = new HandleFamily<>("exchange sink instance", ExchangeSinkInstanceHandle.class, ConnectorHandleCodecProvider::getExchangeSinkInstanceHandleCodec);
    public static final HandleFamily<ExchangeSourceHandle> EXCHANGE_SOURCE = new HandleFamily<>("exchange source", ExchangeSourceHandle.class, ConnectorHandleCodecProvider::getExchangeSourceHandleCodec);
    public static final HandleFamily<ConnectorTableFunctionHandle> TABLE_FUNCTION = new HandleFamily<>("table function", ConnectorTableFunctionHandle.class, ConnectorHandleCodecProvider::getTableFunctionHandleCodec);
    public static final HandleFamily<ConnectorTableCredentials> TABLE_CREDENTIALS = new HandleFamily<>("table credentials", ConnectorTableCredentials.class, ConnectorHandleCodecProvider::getTableCredentialsCodec);

    private static final List<HandleFamily<?>> ALL = ImmutableList.of(
            TABLE,
            COLUMN,
            SPLIT,
            OUTPUT_TABLE,
            INSERT_TABLE,
            TABLE_EXECUTE,
            MERGE_TABLE,
            INDEX,
            TRANSACTION,
            PARTITIONING,
            EXCHANGE_SINK_INSTANCE,
            EXCHANGE_SOURCE,
            TABLE_FUNCTION,
            TABLE_CREDENTIALS);

    private final String name;
    private final Class<T> handleClass;
    private final Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> codecLookup;

    private HandleFamily(String name, Class<T> handleClass, Function<ConnectorHandleCodecProvider, Optional<ConnectorHandleCodec<T>>> codecLookup)
    {
        this.name = requireNonNull(name, "name is null");
        this.handleClass = requireNonNull(handleClass, "handleClass is null");
        this.codecLookup = requireNonNull(codecLookup, "codecLookup is null");
    }

    public static List<HandleFamily<?>> all()
    {
        return ALL;
    }

    public String name()
    {
        return name;
    }

    public Class<T> handleClass()
    {
        return handleClass;
    }

    public Optional<ConnectorHandleCodec<T>> codecFrom(ConnectorHandleCodecProvider provider)
    {
        return requireNonNull(codecLookup.apply(provider), () -> "Codec provider " + provider.getClass().getName() + " returned null for " + name + " handles");
    }

    @Override
    public String toString()
    {
        return name;
    }
}
