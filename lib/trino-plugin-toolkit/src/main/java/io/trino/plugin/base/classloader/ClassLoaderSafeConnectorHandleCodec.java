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
import io.trino.spi.connector.ConnectorHandleCodec;

import static java.util.Objects.requireNonNull;

public final class ClassLoaderSafeConnectorHandleCodec<T>
        implements ConnectorHandleCodec<T>
{
    private final ConnectorHandleCodec<T> delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorHandleCodec(ConnectorHandleCodec<T> delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public byte[] serialize(T handle)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.serialize(handle);
        }
    }

    @Override
    public T deserialize(byte[] bytes)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.deserialize(bytes);
        }
    }
}
