
CREATE DATABASE IF NOT EXISTS logmorph;

CREATE TABLE IF NOT EXISTS logmorph.logs
(
    string_names Array(String),
    string_values Array(String),

    number_names Array(String),
    number_values Array(Float64),

    boolean_names Array(String),
    boolean_values Array(Bool),

    null_names Array(String),

    string_array_names Array(String),
    string_array_values Array(Array(String)),

    number_array_names Array(String),
    number_array_values Array(Array(Float64)),

    boolean_array_names Array(String),
    boolean_array_values Array(Array(Bool)),

    json_names Array(String),
    json_values Array(String),

    raw_source String,

    ingested_at DateTime64(3) DEFAULT now64(3)
)
    ENGINE = MergeTree
    ORDER BY ingested_at;
