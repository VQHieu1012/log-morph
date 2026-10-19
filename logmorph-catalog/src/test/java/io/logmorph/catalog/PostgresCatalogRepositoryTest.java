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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PostgresCatalogRepositoryTest {

    private PostgresCatalogRepository repository;
    private PGSimpleDataSource dataSource;
    private String fieldName;

    @BeforeEach
    void setUp() {
        String url = System.getenv("LOGMORPH_TEST_PG_URL");

        assumeTrue(
                url != null && !url.isBlank(),
                "PostgreSQL integration test URL is not configured"
        );

        dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(
                System.getenv("LOGMORPH_TEST_PG_USER")
        );
        dataSource.setPassword(
                System.getenv("LOGMORPH_TEST_PG_PASSWORD")
        );

        repository = new PostgresCatalogRepository(dataSource);
        fieldName = "test_" + UUID.randomUUID();
    }

    @Test
    void shouldInsertDistinctTypesForSameField() throws Exception {
        Instant observedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(
                                ValueType.STRING, observedAt,
                                ValueType.NUMBER, observedAt
                        )
                )
        );

        assertEquals(
                observedAt,
                readObservedAt(fieldName, ValueType.STRING)
        );

        assertEquals(
                observedAt,
                readObservedAt(fieldName, ValueType.NUMBER)
        );

        assertEquals(2, countRows(fieldName));
    }

    @Test
    void shouldPersistDifferentTimestampsForEachFieldType()
            throws Exception {

        Instant stringObservedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        Instant numberObservedAt =
                Instant.parse("2026-10-19T10:05:00Z");

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(
                                ValueType.STRING, stringObservedAt,
                                ValueType.NUMBER, numberObservedAt
                        )
                )
        );

        assertEquals(
                stringObservedAt,
                readObservedAt(fieldName, ValueType.STRING)
        );

        assertEquals(
                numberObservedAt,
                readObservedAt(fieldName, ValueType.NUMBER)
        );
    }

    @Test
    void shouldNotMoveObservedAtBackwards() throws Exception {
        Instant newer =
                Instant.parse("2026-10-19T10:10:00Z");

        Instant older =
                Instant.parse("2026-10-19T10:00:00Z");

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(ValueType.STRING, newer)
                )
        );

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(ValueType.STRING, older)
                )
        );

        assertEquals(
                newer,
                readObservedAt(fieldName, ValueType.STRING)
        );
    }

    @Test
    void shouldUpdateObservedAtForNewerObservation()
            throws Exception {

        Instant older =
                Instant.parse("2026-10-19T10:00:00Z");

        Instant newer =
                Instant.parse("2026-10-19T10:10:00Z");

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(ValueType.STRING, older)
                )
        );

        repository.persist(
                Map.of(
                        fieldName,
                        Map.of(ValueType.STRING, newer)
                )
        );

        assertEquals(
                newer,
                readObservedAt(fieldName, ValueType.STRING)
        );
    }

    @Test
    void shouldBeIdempotentForRepeatedObservations()
            throws Exception {

        Instant observedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        Map<String, Map<ValueType, Instant>> schema =
                Map.of(
                        fieldName,
                        Map.of(ValueType.STRING, observedAt)
                );

        repository.persist(schema);
        repository.persist(schema);
        repository.persist(schema);

        assertEquals(1, countRows(fieldName));

        assertEquals(
                observedAt,
                readObservedAt(fieldName, ValueType.STRING)
        );
    }

    @Test
    void shouldPersistMultipleFieldsInSingleCall()
            throws Exception {

        Instant t1 =
                Instant.parse("2026-10-19T10:00:00Z");

        Instant t2 =
                Instant.parse("2026-10-19T10:05:00Z");

        String statusField = fieldName + "_status";
        String durationField = fieldName + "_duration";

        repository.persist(
                Map.of(
                        statusField,
                        Map.of(
                                ValueType.STRING, t1,
                                ValueType.NUMBER, t2
                        ),
                        durationField,
                        Map.of(ValueType.NUMBER, t1)
                )
        );

        assertEquals(
                t1,
                readObservedAt(statusField, ValueType.STRING)
        );

        assertEquals(
                t2,
                readObservedAt(statusField, ValueType.NUMBER)
        );

        assertEquals(
                t1,
                readObservedAt(durationField, ValueType.NUMBER)
        );

        assertEquals(2, countRows(statusField));
        assertEquals(1, countRows(durationField));
    }

    @Test
    void shouldRollbackAllRowsWhenBatchContainsInvalidData()
            throws Exception {

        Instant observedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        String validField = fieldName + "_valid";

        // VARCHAR(32) value_type constraint:
        // Use a valid field but force an invalid value through SQL
        // would require changing production code.
        //
        // Instead, use a field name containing a null character.
        // PostgreSQL TEXT rejects embedded NUL characters.

        String invalidField = fieldName + "\u0000invalid";

        Map<String, Map<ValueType, Instant>> schema =
                Map.of(
                        validField,
                        Map.of(ValueType.STRING, observedAt),
                        invalidField,
                        Map.of(ValueType.NUMBER, observedAt)
                );

        assertThrows(
                Exception.class,
                () -> repository.persist(schema)
        );

        assertEquals(0, countRows(validField));
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
                assertTrue(
                        result.next(),
                        "Expected observation was not persisted"
                );

                Timestamp timestamp =
                        result.getTimestamp("observed_at");

                return timestamp.toInstant();
            }
        }
    }

    private long countRows(String name) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                     SELECT COUNT(*)
                     FROM logmorph.schema_catalog
                     WHERE field_name = ?
                     """)) {

            statement.setString(1, name);

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getLong(1);
            }
        }
    }
}
