
package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class SchemaAggregator {

    private final Map<String, EnumMap<ValueType, Instant>> schema =
            new HashMap<>();

    private final Map<String, EnumMap<ValueType, Instant>> pendingDiscovery =
            new HashMap<>();

    public boolean hasPendingDiscovery() {
        return !pendingDiscovery.isEmpty();
    }

    public void add(
            String fieldName,
            ValueType valueType,
            Instant observedAt
    ) {
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(valueType);
        Objects.requireNonNull(observedAt);

        EnumMap<ValueType, Instant> types = schema.computeIfAbsent(
                fieldName,
                ignored -> new EnumMap<>(ValueType.class)
        );

        Instant existing = types.get(valueType);

        if (existing == null) {
            types.put(valueType, observedAt);

            pendingDiscovery.computeIfAbsent(
                    fieldName,
                    ignored -> new EnumMap<>(ValueType.class)
            ).put(valueType, observedAt);

            return;
        }

        if (observedAt.isAfter(existing)) {
            types.put(valueType, observedAt);

            EnumMap<ValueType, Instant> pendingTypes =
                    pendingDiscovery.get(fieldName);

            if (pendingTypes != null
                    && pendingTypes.containsKey(valueType)) {
                pendingTypes.put(valueType, observedAt);
            }
        }
    }

    public Map<String, Map<ValueType, Instant>> snapshot() {
        Map<String, Map<ValueType, Instant>> result = new HashMap<>();

        schema.forEach(
                (fieldName, types) ->
                        result.put(fieldName, Map.copyOf(types))
        );

        return Map.copyOf(result);
    }

    public Map<String, Map<ValueType, Instant>> discoverySnapshot() {
        Map<String, Map<ValueType, Instant>> result = new HashMap<>();

        pendingDiscovery.forEach(
                (fieldName, types) ->
                        result.put(fieldName, Map.copyOf(types))
        );

        return Map.copyOf(result);
    }

    public void markDiscoveryPersisted() {
        pendingDiscovery.clear();
    }
}
