package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class PostgresCatalogRepository implements CatalogRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO logmorph.schema_catalog (
                field_name,
                value_type,
                observed_at
            )
            VALUES (?, ?, ?)
            ON CONFLICT (field_name, value_type)
            DO UPDATE SET
                observed_at = EXCLUDED.observed_at
            WHERE logmorph.schema_catalog.observed_at
                    < EXCLUDED.observed_at
            """;
    private final DataSource dataSource;

    public PostgresCatalogRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    @Override
    public void persist(
            Map<String, Set<ValueType>> schema,
            Instant observedAt
    ) throws SQLException {
        Objects.requireNonNull(schema);
        Objects.requireNonNull(observedAt);

        if (schema.isEmpty()) return;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit((false));

            try (PreparedStatement statement = connection.prepareStatement(UPSERT_SQL)) {
                Timestamp timestamp = Timestamp.from(observedAt);

                for (Map.Entry<String, Set<ValueType>> entry : schema.entrySet()) {
                    for (ValueType valueType : entry.getValue()) {
                        statement.setString(1, entry.getKey());
                        statement.setString(2, valueType.name());
                        statement.setTimestamp(3, timestamp);
                        statement.addBatch();
                    }

                    statement.executeBatch();
                    connection.commit();
                }
            } catch (SQLException | RuntimeException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }

                throw e;
            }
        }
    }
}
