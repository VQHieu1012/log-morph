package io.logmorph.query;

import io.logmorph.model.ValueType;
import io.logmorph.query.catalog.PostgresSchemaCatalogReader;
import io.logmorph.query.catalog.SchemaCatalogReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresSchemaCatalogReaderTest {
    private DataSource dataSource;
    private SchemaCatalogReader reader;
    private String fieldPrefix;

    @BeforeEach
    void setUp() {
        String url = System.getenv("LOGMORPH_TEST_PG_URL");

        assumeTrue(
                url != null && !url.isBlank(),
                "PostgreSQL integration test is not configured"
        );

        PGSimpleDataSource postgres = new PGSimpleDataSource();

        postgres.setUrl(url);
        postgres.setUser(System.getenv("LOGMORPH_TEST_PG_USER"));
        postgres.setPassword(System.getenv("LOGMORPH_TEST_PG_PASSWORD"));

        dataSource = postgres;
        reader = new PostgresSchemaCatalogReader(dataSource);

        fieldPrefix = "reader_test_" + UUID.randomUUID();
    }

    @Test
    void shouldResolveMultipleTypesForEachField() throws Exception {
        String status = fieldPrefix + "_status";
        String duration = fieldPrefix + "_duration";

        insert(status, ValueType.STRING);
        insert(status, ValueType.NUMBER);
        insert(duration, ValueType.NUMBER);

        Map<String, Set<ValueType>> result = reader.getFieldTypes(
                Set.of(status, duration)
        );

        assertEquals(
                Set.of(ValueType.STRING, ValueType.NUMBER),
                result.get(status)
        );

        assertEquals(
                Set.of(ValueType.NUMBER),
                result.get(duration)
        );

        assertEquals(2, result.size());
    }

    private void insert(
            String fieldName, ValueType valueType
    ) throws Exception {
        String sql = """
                INSERT INTO logmorph.schema_catalog (
                    field_name,
                    value_type,
                    observed_at
                ) VALUES (?, ?, ?)
                """;

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(sql)) {
            statement.setString(1, fieldName);
            statement.setString(2, valueType.name());
            statement.setObject(3, java.sql.Timestamp.from(Instant.now()));

            statement.executeUpdate();
        }
    }
}
