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
package io.trino.server;

/**
 * Wire encoding for the coordinator-to-worker task control plane
 * ({@code /v1/task} endpoints). This is a cluster-wide benchmarking switch,
 * not per-request negotiation: every node runs the same build and the same
 * setting, so both ends always agree on the format.
 */
public enum ControlPlaneEncoding
{
    JSON,
    PROTOBUF,
}
