package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.Map;

public interface CatalogRepository {

    void persist(
            Map<String, Map<ValueType, Instant>> schema
    ) throws Exception;
}
