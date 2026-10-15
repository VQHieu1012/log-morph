package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaObserverTest {
    private final SchemaObserver observer = new SchemaObserver();

    @Test
    void shouldObserveAllTypesDirectlyIntoAggregator() {
        SchemaAggregator aggregator = getSchemaAggregator();

        assertEquals(
                Map.of(
                        "service", Set.of(ValueType.STRING),
                        "duration", Set.of(ValueType.NUMBER),
                        "success", Set.of(ValueType.BOOLEAN),
                        "error", Set.of(ValueType.NULL),
                        "tags", Set.of(ValueType.STRING_ARRAY),
                        "scores", Set.of(ValueType.NUMBER_ARRAY),
                        "flags", Set.of(ValueType.BOOLEAN_ARRAY),
                        "items", Set.of(ValueType.JSON_FRAGMENT)
                ),
                aggregator.snapshot()
        );
    }

    private SchemaAggregator getSchemaAggregator() {
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

        observer.observeInto(log, aggregator);
        return aggregator;
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

        observer.observeInto(log, aggregator);

        assertEquals(
                Map.of(
                        "status",
                        Set.of(ValueType.STRING, ValueType.NUMBER)
                ),
                aggregator.snapshot()
        );
    }
}
