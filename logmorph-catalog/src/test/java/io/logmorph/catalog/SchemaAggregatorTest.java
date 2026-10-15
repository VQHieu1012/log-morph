package io.logmorph.catalog;

import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaAggregatorTest {

    @Test
    void shouldMergeTypesAcrossLogs() {
        SchemaAggregator aggregator = new SchemaAggregator();

        aggregator.add("status", ValueType.STRING);
        aggregator.add("duration", ValueType.NUMBER);

        aggregator.add("status", ValueType.NUMBER);
        aggregator.add("duration", ValueType.NUMBER);

        aggregator.add("status", ValueType.NULL);
        aggregator.add("success", ValueType.BOOLEAN);

        assertEquals(
                Map.of(
                        "status", Set.of(
                                ValueType.STRING,
                                ValueType.NUMBER,
                                ValueType.NULL
                        ),
                        "duration", Set.of(ValueType.NUMBER),
                        "success", Set.of(ValueType.BOOLEAN)
                ),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldIgnoreRepeatedFieldTypes() {
        SchemaAggregator aggregator = new SchemaAggregator();

        aggregator.add("status", ValueType.STRING);
        aggregator.add("status", ValueType.STRING);
        aggregator.add("status", ValueType.STRING);

        assertEquals(
                Map.of("status", Set.of(ValueType.STRING)),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldReturnIndependentSnapshot() {
        SchemaAggregator aggregator = new SchemaAggregator();

        aggregator.add("status", ValueType.STRING);

        Map<String, Set<ValueType>> firstSnapshot = aggregator.snapshot();

        aggregator.add("status", ValueType.NUMBER);

        assertEquals(
                Set.of(ValueType.STRING),
                firstSnapshot.get("status")
        );

        assertEquals(
                Set.of(ValueType.STRING, ValueType.NUMBER),
                aggregator.snapshot().get("status")
        );
    }
}
