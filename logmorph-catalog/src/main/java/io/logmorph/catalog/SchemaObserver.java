package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;

import java.util.List;

public final class SchemaObserver {

    private static void addInto(
            SchemaAggregator aggregator,
            List<String> fieldNames,
            ValueType valueType
    ) {
        for (String fieldName : fieldNames) {
            aggregator.add(fieldName, valueType);
        }
    }

    public void observeInto(
            TypedLog log,
            SchemaAggregator aggregator
    ) {
        addInto(aggregator, log.stringNames(), ValueType.STRING);
        addInto(aggregator, log.numberNames(), ValueType.NUMBER);
        addInto(aggregator, log.booleanNames(), ValueType.BOOLEAN);
        addInto(aggregator, log.nullNames(), ValueType.NULL);

        addInto(aggregator, log.stringArrayNames(), ValueType.STRING_ARRAY);
        addInto(aggregator, log.numberArrayNames(), ValueType.NUMBER_ARRAY);
        addInto(aggregator, log.booleanArrayNames(), ValueType.BOOLEAN_ARRAY);
        addInto(aggregator, log.jsonNames(), ValueType.JSON_FRAGMENT);
    }
}
