package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public final class SchemaAggregator {

    private final Map<String, EnumMap<ValueType, Instant>> schema = new HashMap<>();

    public void add(String fieldName, ValueType valueType, Instant observedAt) {
        schema.computeIfAbsent(
                fieldName,
                ignored -> new EnumMap<>(ValueType.class)
        ).merge(
                valueType,
                observedAt,
                (existing, incoming) -> existing.isAfter(incoming) ? existing : incoming
        );
    }

    public Map<String, Map<ValueType, Instant>> snapshot() {
        Map<String, Map<ValueType, Instant>> result = new HashMap<>();

        schema.forEach(
                (fieldName, types) ->
                        result.put(fieldName, Map.copyOf(types))
        );

        return Map.copyOf(result);
    }
}
