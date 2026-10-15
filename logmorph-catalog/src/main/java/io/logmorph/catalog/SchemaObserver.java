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
}

