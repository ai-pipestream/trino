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
package io.trino.protobuf.handles;

import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;

/**
 * Fixed set of types for codec tests, resolved by {@code TypeId}. Connector
 * tests that cannot depend on the engine's type manager use it through this
 * module's test jar and add the types their handles carry.
 */
public final class TestingTypeManager
        implements TypeManager
{
    public static final Type LONG_DECIMAL = createDecimalType(38, 2);
    public static final Type SHORT_DECIMAL = createDecimalType(10, 2);
    public static final Type VARCHAR_10 = createVarcharType(10);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    // comparable but not orderable, so domains over it use EquatableValueSet
    public static final Type BIGINT_MAP = new MapType(BIGINT, BIGINT, TYPE_OPERATORS);

    private static final List<Type> DEFAULT_TYPES = List.of(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARCHAR, VARCHAR_10, VARBINARY, DATE, TIMESTAMP_MILLIS, TIMESTAMP_NANOS, SHORT_DECIMAL, LONG_DECIMAL, BIGINT_MAP);

    private final Map<String, Type> types;

    public TestingTypeManager()
    {
        this(List.of());
    }

    public TestingTypeManager(List<Type> additionalTypes)
    {
        this.types = Stream.concat(DEFAULT_TYPES.stream(), additionalTypes.stream())
                .collect(Collectors.toMap(type -> type.getTypeId().getId(), Function.identity(), (left, _) -> left));
    }

    @Override
    public Type getType(TypeDescriptor descriptor)
    {
        return fromSqlType(descriptor.toString());
    }

    @Override
    public Type fromSqlType(String type)
    {
        Type result = types.get(type);
        if (result == null) {
            throw new IllegalArgumentException("Unknown type: " + type);
        }
        return result;
    }

    @Override
    public boolean isTypeRegistered(String name)
    {
        return types.containsKey(name);
    }

    @Override
    public TypeOperators getTypeOperators()
    {
        return TYPE_OPERATORS;
    }
}
