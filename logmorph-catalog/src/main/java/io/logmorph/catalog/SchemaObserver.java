package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.List;

public final class SchemaObserver {

    private static void addInto(
            SchemaAggregator aggregator,
            List<String> fieldNames,
            ValueType valueType,
            Instant observedAt
    ) {
        for (String fieldName : fieldNames) {
            aggregator.add(fieldName, valueType, observedAt);
        }
    }

    public void observeInto(
            TypedLog log,
            Instant observedAt,
            SchemaAggregator aggregator
    ) {
        addInto(aggregator, log.stringNames(), ValueType.STRING, observedAt);
        addInto(aggregator, log.numberNames(), ValueType.NUMBER, observedAt);
        addInto(aggregator, log.booleanNames(), ValueType.BOOLEAN, observedAt);
        addInto(aggregator, log.nullNames(), ValueType.NULL, observedAt);

        addInto(aggregator, log.stringArrayNames(), ValueType.STRING_ARRAY, observedAt);
        addInto(aggregator, log.numberArrayNames(), ValueType.NUMBER_ARRAY, observedAt);
        addInto(aggregator, log.booleanArrayNames(), ValueType.BOOLEAN_ARRAY, observedAt);
        addInto(aggregator, log.jsonNames(), ValueType.JSON_FRAGMENT, observedAt);
    }
}
