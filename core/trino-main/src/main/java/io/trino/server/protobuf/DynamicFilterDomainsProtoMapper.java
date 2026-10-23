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

import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DynamicFilterDomainsProtoMapper
{
    private final DomainProtoMapper domainProtoMapper;

    @Inject
    public DynamicFilterDomainsProtoMapper(DomainProtoMapper domainProtoMapper)
    {
        this.domainProtoMapper = requireNonNull(domainProtoMapper, "domainProtoMapper is null");
    }

    public io.trino.server.protocol.v1.VersionedDynamicFilterDomains toProto(VersionedDynamicFilterDomains domains)
    {
        io.trino.server.protocol.v1.VersionedDynamicFilterDomains.Builder builder = io.trino.server.protocol.v1.VersionedDynamicFilterDomains.newBuilder()
                .setVersion(domains.getVersion());
        domains.getDynamicFilterDomains().forEach((id, domain) ->
                builder.putDynamicFilterDomains(id.toString(), domainProtoMapper.toProto(domain)));
        return builder.build();
    }

    public VersionedDynamicFilterDomains fromProto(io.trino.server.protocol.v1.VersionedDynamicFilterDomains proto)
    {
        Map<DynamicFilterId, Domain> domains = proto.getDynamicFilterDomainsMap().entrySet().stream()
                .collect(toImmutableMap(entry -> new DynamicFilterId(entry.getKey()), entry -> domainProtoMapper.fromProto(entry.getValue())));
        return new VersionedDynamicFilterDomains(proto.getVersion(), domains);
    }

    public byte[] toByteArray(VersionedDynamicFilterDomains domains)
    {
        return toProto(domains).toByteArray();
    }

    public VersionedDynamicFilterDomains parseFrom(byte[] bytes)
    {
        try {
            return fromProto(io.trino.server.protocol.v1.VersionedDynamicFilterDomains.parseFrom(bytes));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to decode protobuf VersionedDynamicFilterDomains", e);
        }
    }
}
