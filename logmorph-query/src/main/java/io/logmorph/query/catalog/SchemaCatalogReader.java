package io.logmorph.query.catalog;

import io.logmorph.model.ValueType;

import java.util.Map;
import java.util.Set;

public interface SchemaCatalogReader {
    Map<String, Set<ValueType>> getFieldTypes(
            Set<String> fieldNames
    ) throws Exception;
}
