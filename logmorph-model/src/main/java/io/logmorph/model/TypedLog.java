package io.logmorph.model;

import java.util.List;

public record TypedLog(
        List<String> stringNames,
        List<String> stringValues,

        List<String> numberNames,
        List<Double> numberValues,

        List<String> booleanNames,
        List<Boolean> booleanValues,

        List<String> nullNames,

        List<String> stringArrayNames,
        List<List<String>> stringArrayValues,

        List<String> numberArrayNames,
        List<List<Double>> numberArrayValues,

        List<String> booleanArrayNames,
        List<List<Boolean>> booleanArrayValues,

        List<String> jsonNames,
        List<String> jsonValues,

        String rawSource
) {
    public TypedLog {
        validatePair(stringNames, stringValues, "STRING");
        validatePair(numberNames, numberValues, "NUMBER");
        validatePair(booleanNames, booleanValues, "BOOLEAN");
        validatePair(stringArrayNames, stringArrayValues, "STRING_ARRAY");
        validatePair(numberArrayNames, numberArrayValues, "NUMBER_ARRAY");
        validatePair(jsonNames, jsonValues, "JSON_FRAGMENT");
        validatePair(booleanArrayNames, booleanArrayValues, "BOOLEAN_ARRAY");
    }

    private static void validatePair(
            List<String> names,
            List<?> values,
            String type
    ) {
        if (names.size() != values.size()) {
            throw new IllegalArgumentException(
                    type + " names and values must have the same size"
            );
        }
    }
}