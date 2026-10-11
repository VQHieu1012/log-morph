package io.logmorph.model;

import java.util.Objects;

public record SchemaObservation(
        String fieldName,
        ValueType valueType
) {
    public SchemaObservation {
        Objects.requireNonNull(fieldName, "fieldName");
        Objects.requireNonNull(valueType, "valueType");

        if (fieldName.isBlank()) {
            throw new IllegalArgumentException(
                    "fieldName must not be blank"
            );
        }
    }
}
