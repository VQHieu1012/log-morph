package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class SchemaAggregator {

    private final Map<String, Set<ValueType>> schema = new HashMap<>();

    public void add(String fieldName, ValueType valueType) {
        schema.computeIfAbsent(
                fieldName,
                ignored -> new HashSet<>()
        ).add(valueType);
    }

    public Map<String, Set<ValueType>> snapshot() {
        Map<String, Set<ValueType>> result = new HashMap<>();

        schema.forEach(
                (fieldName, types) ->
                        result.put(fieldName, Set.copyOf(types))
        );

        return Map.copyOf(result);
    }
}
