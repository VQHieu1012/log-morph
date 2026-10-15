package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

public interface CatalogRepository {

    void persist(
            Map<String, Set<ValueType>> schema,
            Instant observedAt
    ) throws Exception;
}
