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

/**
 * Binary codec for one connector handle type, used when the task control
 * plane runs with protobuf encoding. The bytes are the connector's own
 * protobuf message; the SPI itself stays free of a protobuf dependency.
 * The bytes must never contain a JSON representation of the handle.
 */
public interface ConnectorHandleCodec<T>
{
    byte[] serialize(T handle);

    T deserialize(byte[] bytes);
}
