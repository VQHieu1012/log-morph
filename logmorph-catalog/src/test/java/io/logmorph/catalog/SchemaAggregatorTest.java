package io.logmorph.catalog;

import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaAggregatorTest {

    @Test
    void shouldMergeTypesWithTheirObservedTimestamps() {
        SchemaAggregator aggregator = new SchemaAggregator();

        Instant t1 = Instant.parse("2026-10-19T10:00:00Z");
        Instant t2 = Instant.parse("2026-10-19T10:05:00Z");

        aggregator.add("status", ValueType.STRING, t1);
        aggregator.add("status", ValueType.NUMBER, t2);
        aggregator.add("duration", ValueType.NUMBER, t1);

        assertEquals(
                Map.of(
                        "status", Map.of(
                                ValueType.STRING, t1,
                                ValueType.NUMBER, t2
                        ),
                        "duration", Map.of(ValueType.NUMBER, t1)
                ),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldKeepLatestTimestampRegardlessOfArrivalOrder() {
        SchemaAggregator aggregator = new SchemaAggregator();

        Instant older = Instant.parse("2026-10-19T10:00:00Z");
        Instant newer = Instant.parse("2026-10-19T10:10:00Z");

        aggregator.add("status", ValueType.STRING, newer);
        aggregator.add("status", ValueType.STRING, older);

        assertEquals(
                newer,
                aggregator.snapshot()
                        .get("status")
                        .get(ValueType.STRING)
        );
    }

    @Test
    void shouldReturnIndependentSnapshot() {
        SchemaAggregator aggregator = new SchemaAggregator();

        Instant t1 = Instant.parse("2026-10-19T10:00:00Z");
        Instant t2 = Instant.parse("2026-10-19T10:10:00Z");

        aggregator.add("status", ValueType.STRING, t1);

        Map<String, Map<ValueType, Instant>> firstSnapshot =
                aggregator.snapshot();

        aggregator.add("status", ValueType.STRING, t2);

        assertEquals(
                t1,
                firstSnapshot.get("status").get(ValueType.STRING)
        );

        assertEquals(
                t2,
                aggregator.snapshot()
                        .get("status")
                        .get(ValueType.STRING)
        );
    }
}
