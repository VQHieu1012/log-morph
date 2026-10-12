package io.logmorph.ingestion.sink;

import java.util.List;

final class ClickHouseInsertColumns {

    static final List<String> NAMES = List.of(
            "string_names",
            "string_values",
            "number_names",
            "number_values",
            "boolean_names",
            "boolean_values",
            "null_names",
            "string_array_names",
            "string_array_values",
            "number_array_names",
            "number_array_values",
            "boolean_array_names",
            "boolean_array_values",
            "json_names",
            "json_values",
            "raw_source"
    );

    private ClickHouseInsertColumns() {
    }
}