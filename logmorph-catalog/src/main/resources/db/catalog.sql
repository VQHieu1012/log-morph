CREATE SCHEMA IF NOT EXISTS logmorph;

CREATE TABLE IF NOT EXISTS logmorph.schema_catalog (
    field_name   TEXT NOT NULL,
    value_type   VARCHAR(32) NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (field_name, value_type)
);