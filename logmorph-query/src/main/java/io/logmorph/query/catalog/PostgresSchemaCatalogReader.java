package io.logmorph.query.catalog;

import io.logmorph.model.ValueType;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public final class PostgresSchemaCatalogReader
        implements SchemaCatalogReader {
    private static final String SELECT_FIELD_TYPES_SQL = """
            SELECT field_name, value_type
            FROM logmorph.schema_catalog
            WHERE field_name = ANY (?)
            """;

    private final DataSource dataSource;

    public PostgresSchemaCatalogReader(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    @Override
    public Map<String, Set<ValueType>> getFieldTypes(Set<String> fieldNames) throws Exception {
        Objects.requireNonNull(fieldNames);

        if (fieldNames.isEmpty()) {
            return Map.of();
        }

        Map<String, Set<ValueType>> result = new HashMap<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_FIELD_TYPES_SQL)
        ) {
            Array names = connection.createArrayOf(
                    "text", fieldNames.toArray(String[]::new)
            );

            try {
                statement.setArray(1, names);

                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        String fieldName = rs.getString("field_name");
                        ValueType valueType = ValueType.valueOf(rs.getString(("value_type")));

                        result.computeIfAbsent(
                                fieldName,
                                ignored -> EnumSet.noneOf(ValueType.class)
                        ).add(valueType);
                    }
                }
            } finally {
                names.free();
            }
        }

        Map<String, Set<ValueType>> immutable = new HashMap<>();

        result.forEach(
                (fieldName, types) -> immutable.put(fieldName, Set.copyOf(types))
        );
        return Map.copyOf((immutable));
    }

}
