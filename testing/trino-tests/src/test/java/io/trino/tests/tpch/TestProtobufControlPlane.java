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
package io.trino.tests.tpch;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

/**
 * Runs distributed queries with the task control plane switched to protobuf
 * ({@code internal-communication.control-plane.encoding=PROTOBUF}). Task
 * status polls and dynamic filter fetches between coordinator and workers
 * must travel as protobuf and produce identical results.
 */
public class TestProtobufControlPlane
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder()
                .addExtraProperty("internal-communication.control-plane.encoding", "PROTOBUF")
                .build();
    }

    @Test
    public void testSimpleScan()
    {
        assertQuery("SELECT count(*) FROM nation WHERE nationkey > 10");
    }

    @Test
    public void testJoinWithDynamicFiltering()
    {
        // Exercises dynamic filter collection: build-side domains are fetched
        // from workers over GET /{taskId}/dynamicfilters.
        assertQuery("" +
                "SELECT count(*) " +
                "FROM orders o JOIN customer c ON o.custkey = c.custkey " +
                "WHERE c.name LIKE 'Customer#0000001%'");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("" +
                "SELECT orderstatus, count(*), sum(totalprice) " +
                "FROM orders " +
                "GROUP BY orderstatus " +
                "ORDER BY orderstatus");
    }
}
