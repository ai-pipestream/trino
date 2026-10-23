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

import io.trino.connector.CatalogHandle;
import io.trino.execution.TaskId;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.transaction.TransactionId;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Parsers for the engine identifiers that protobuf messages carry as
 * strings. Each turns a malformed value into a categorized Trino error
 * naming the value, instead of the bare {@link IllegalArgumentException}
 * the identifier classes throw.
 */
public final class ProtoIdentifiers
{
    private ProtoIdentifiers() {}

    public static CatalogHandle parseCatalogHandle(String id)
    {
        try {
            return CatalogHandle.fromId(id);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf message carries an invalid catalog handle id: " + id, e);
        }
    }

    public static HostAddress parseHostAddress(String address)
    {
        try {
            return HostAddress.fromString(address);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf message carries an invalid host address: " + address, e);
        }
    }

    public static TransactionId parseTransactionId(String transactionId)
    {
        try {
            return TransactionId.valueOf(transactionId);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf message carries an invalid transaction id: " + transactionId, e);
        }
    }

    public static TaskId parseTaskId(String taskId)
    {
        try {
            return TaskId.valueOf(taskId);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Protobuf message carries an invalid task id: " + taskId, e);
        }
    }
}
