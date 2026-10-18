package io.logmorph.catalog;

import io.logmorph.model.ValueType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresCatalogRepositoryTest {
    private PostgresCatalogRepository repository;
    private PGSimpleDataSource dataSource;
    private String fieldName;

    @BeforeEach
    void setUp() {
        String url = System.getenv("LOGMORPH_TEST_PG_URL");

        assumeTrue(url != null && !url.isBlank());

        dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(System.getenv("LOGMORPH_TEST_PG_USER"));
        dataSource.setPassword(System.getenv("LOGMORPH_TEST_PG_PASSWORD"));
        repository = new PostgresCatalogRepository(dataSource);
        fieldName = "test_" + UUID.randomUUID();
    }

    @Test
    void shouldInsertDistinctTypesForSameField() throws Exception {
        Instant observedAt = Instant.parse("2026-10-16T12:00:00Z");

        repository.persist(
                Map.of("status_" + fieldName,
                        Set.of(ValueType.STRING, ValueType.NUMBER)),
                observedAt
        );

        assertEquals(
                observedAt,
                readObservedAt("status_" + fieldName, ValueType.STRING)
        );

        assertEquals(
                observedAt,
                readObservedAt("status_" + fieldName, ValueType.NUMBER)
        );
    }

    @Test
    void shouldNotMoveObservedAtBackwards() throws Exception {
        Instant newer = Instant.parse("2026-10-16T12:10:00Z");
        Instant older = Instant.parse("2026-10-16T12:00:00Z");

        Map<String, Set<ValueType>> schema =
                Map.of(fieldName, Set.of(ValueType.STRING));

        repository.persist(schema, newer);
        repository.persist(schema, older);

        assertEquals(
                newer,
                readObservedAt(fieldName, ValueType.STRING)
        );
    }

    @Test
    void shouldUpdateObservedAtForNewerObservation() throws Exception {
        Instant older = Instant.parse("2026-10-16T12:00:00Z");
        Instant newer = Instant.parse("2026-10-16T12:10:00Z");

        Map<String, Set<ValueType>> schema =
                Map.of(fieldName, Set.of(ValueType.STRING));

        repository.persist(schema, older);
        repository.persist(schema, newer);

        assertEquals(
                newer,
                readObservedAt(fieldName, ValueType.STRING)
        );
    }

    @Test
    void shouldBeIdempotentForRepeatedObservations() throws Exception {
        Instant observedAt = Instant.parse("2026-10-16T12:00:00Z");

        Map<String, Set<ValueType>> schema =
                Map.of(fieldName, Set.of(ValueType.STRING));

        repository.persist(schema, observedAt);
        repository.persist(schema, observedAt);

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                     SELECT count(*)
                     FROM logmorph.schema_catalog
                     WHERE field_name = ?
                       AND value_type = ?
                     """)) {

            statement.setString(1, fieldName);
            statement.setString(2, ValueType.STRING.name());

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(1, result.getLong(1));
            }
        }
    }

    private Instant readObservedAt(
            String name,
            ValueType valueType
    ) throws Exception {

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                     SELECT observed_at
                     FROM logmorph.schema_catalog
                     WHERE field_name = ?
                       AND value_type = ?
                     """)) {

            statement.setString(1, name);
            statement.setString(2, valueType.name());

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());

                Timestamp timestamp =
                        result.getTimestamp("observed_at");

                return timestamp.toInstant();
            }
        }
    }


}
