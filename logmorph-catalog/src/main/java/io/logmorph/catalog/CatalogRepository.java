package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.Map;

public interface CatalogRepository {

    void persist(
            Map<String, Map<ValueType, Instant>> schema
    ) throws Exception;

    default void persistDiscovery(
            Map<String, Map<ValueType, Instant>> discovered
    ) throws Exception {
        persist(discovered);
    }

    default void refreshObservedAt(
            Map<String, Map<ValueType, Instant>> schema
    ) throws Exception {
        throw new UnsupportedOperationException(
                "Freshness persistence is not supported"
        );
    }
}
