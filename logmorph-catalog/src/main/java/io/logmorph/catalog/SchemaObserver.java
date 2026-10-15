package io.logmorph.catalog;

import io.logmorph.model.SchemaObservation;
import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaObserver {

    public Set<SchemaObservation> observe(TypedLog log) {
        Set<SchemaObservation> observations = new HashSet<>();

        addObservations(observations, log.stringNames(), ValueType.STRING);
        addObservations(observations, log.numberNames(), ValueType.NUMBER);
        addObservations(observations, log.booleanNames(), ValueType.BOOLEAN);
        addObservations(observations, log.nullNames(), ValueType.NULL);
        addObservations(observations, log.stringArrayNames(), ValueType.STRING_ARRAY);
        addObservations(observations, log.numberArrayNames(), ValueType.NUMBER_ARRAY);
        addObservations(observations, log.booleanArrayNames(), ValueType.BOOLEAN_ARRAY);
        addObservations(observations, log.jsonNames(), ValueType.JSON_FRAGMENT);

        return observations;
    }

    private void addObservations(
            Set<SchemaObservation> observations,
            List<String> fieldNames,
            ValueType fieldType
    ) {
        for (String fieldName : fieldNames) {
            observations.add(new SchemaObservation(fieldName, fieldType));
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

    private static void addInto(
            SchemaAggregator aggregator,
            List<String> fieldNames,
            ValueType valueType
    ) {
        for (String fieldName : fieldNames) {
            aggregator.add(fieldName, valueType);
        }
    }
}

