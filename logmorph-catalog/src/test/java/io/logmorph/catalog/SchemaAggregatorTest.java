package io.logmorph.catalog;

import io.logmorph.model.SchemaObservation;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaAggregatorTest {

    @Test
    void shouldMergeObservationsAcrossLogs() {
        SchemaAggregator aggregator = new SchemaAggregator();

        aggregator.add(Set.of(
                new SchemaObservation("status", ValueType.STRING),
                new SchemaObservation("duration", ValueType.NUMBER)
        ));

        aggregator.add(Set.of(
                new SchemaObservation("status", ValueType.NUMBER),
                new SchemaObservation("duration", ValueType.NUMBER)
        ));

        aggregator.add(Set.of(
                new SchemaObservation("status", ValueType.NULL),
                new SchemaObservation("success", ValueType.BOOLEAN)
        ));

        assertEquals(
                Map.of(
                        "status", Set.of(ValueType.STRING, ValueType.NUMBER, ValueType.NULL),
                        "duration", Set.of(ValueType.NUMBER),
                        "success", Set.of(ValueType.BOOLEAN)
                ),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldIgnoreRepeatedObservations() {
        SchemaAggregator aggregator = new SchemaAggregator();

        Set<SchemaObservation> observations = Set.of(new SchemaObservation("status", ValueType.STRING));

        aggregator.add(observations);
        aggregator.add(observations);
        aggregator.add(observations);

        assertEquals(
                Map.of("status", Set.of(ValueType.STRING)),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldReturnIndependentSnapshot() {
        SchemaAggregator aggregator = new SchemaAggregator();

        aggregator.add(Set.of(new SchemaObservation("status", ValueType.STRING)));

        Map<String, Set<ValueType>> firstSnapshot = aggregator.snapshot();

        aggregator.add(Set.of(new SchemaObservation("status", ValueType.NUMBER)));

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
