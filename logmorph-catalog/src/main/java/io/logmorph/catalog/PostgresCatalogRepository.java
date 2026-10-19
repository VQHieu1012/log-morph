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
            Map<String, Map<ValueType, Instant>> schema
    ) throws SQLException {
        Objects.requireNonNull(schema);

        if (schema.isEmpty()) return;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit((false));

            try (PreparedStatement statement = connection.prepareStatement(UPSERT_SQL)) {
                for (Map.Entry<String, Map<ValueType, Instant>> entry : schema.entrySet()) {
                    for (Map.Entry<ValueType, Instant> observation : entry.getValue().entrySet()) {
                        statement.setString(1, entry.getKey());
                        statement.setString(2, observation.getKey().name());
                        statement.setTimestamp(3, Timestamp.from(observation.getValue()));
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
