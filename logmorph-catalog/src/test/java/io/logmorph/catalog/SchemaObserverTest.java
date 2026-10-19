package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaObserverTest {
    private final SchemaObserver observer = new SchemaObserver();

    @Test
    void shouldObserveAllTypesDirectlyIntoAggregator() {

        TypedLog log = new TypedLog(
                List.of("service"),
                List.of("payment"),

                List.of("duration"),
                List.of(125.5),

                List.of("success"),
                List.of(true),

                List.of("error"),

                List.of("tags"),
                List.of(List.of("api", "payment")),

                List.of("scores"),
                List.of(List.of(10.5, 20.5)),

                List.of("flags"),
                List.of(List.of(true, false)),

                List.of("items"),
                List.of("[{\"id\":1}]"),

                "{}"
        );

        SchemaAggregator aggregator = new SchemaAggregator();

        Instant observedAt = Instant.parse("2026-10-19T10:00:00Z");

        observer.observeInto(log, observedAt, aggregator);

        assertEquals(
                Map.of(
                        "service", Map.of(ValueType.STRING, observedAt),
                        "duration", Map.of(ValueType.NUMBER, observedAt),
                        "success", Map.of(ValueType.BOOLEAN, observedAt),
                        "error", Map.of(ValueType.NULL, observedAt),
                        "tags", Map.of(ValueType.STRING_ARRAY, observedAt),
                        "scores", Map.of(ValueType.NUMBER_ARRAY, observedAt),
                        "flags", Map.of(ValueType.BOOLEAN_ARRAY, observedAt),
                        "items", Map.of(ValueType.JSON_FRAGMENT, observedAt)
                ),
                aggregator.snapshot()
        );
    }

    @Test
    void shouldMergeDuplicateFieldsDirectly() {
        TypedLog log = new TypedLog(
                List.of("status", "status"),
                List.of("ok", "success"),

                List.of("status"),
                List.of(200.0),

                List.of(),
                List.of(),

                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                "{}"
        );

        SchemaAggregator aggregator = new SchemaAggregator();

        Instant observedAt = Instant.parse("2026-10-19T10:00:00Z");

        observer.observeInto(log, observedAt, aggregator);

        assertEquals(
                Map.of(
                        "status", Map.of(
                                ValueType.STRING, observedAt,
                                ValueType.NUMBER, observedAt
                        )
                ),
                aggregator.snapshot()
        );
    }
}
