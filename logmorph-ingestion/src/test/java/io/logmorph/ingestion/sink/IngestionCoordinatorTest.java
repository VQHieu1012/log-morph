
package io.logmorph.ingestion;

import com.clickhouse.client.api.Client;
import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.parser.JacksonLogParser;
import io.logmorph.ingestion.sink.ClickHouseBatchWriter;
import io.logmorph.ingestion.sink.IngestionCoordinator;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IngestionCoordinatorTest {

    private static final String TABLE = "logmorph.logs";

    private Client createClient() {
        return new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("logmorph")
                .setPassword("logmorph123")
                .compressServerResponse(false)
                .serverSetting(
                        "http_x_clickhouse_format_overrides_output_format",
                        "0"
                )
                .serverSetting("async_insert", "0")
                .build();
    }

    private IngestionCoordinator createCoordinator(
            Client client,
            int batchSize
    ) {
        return new IngestionCoordinator(
                new JacksonLogParser(),
                new BatchAccumulator(batchSize),
                new ClickHouseBatchWriter(client, TABLE)
        );
    }

    private long countByMarker(Client client, String marker) {
        String sql = """
                SELECT count() AS total
                FROM logmorph.logs
                WHERE has(string_values, '%s')
                """.formatted(marker);

        return client.queryAll(sql).get(0).getLong("total");
    }

    @Test
    void shouldAutomaticallyFlushWhenBatchIsFull() throws Exception {
        String marker = UUID.randomUUID().toString();

        try (Client client = createClient()) {
            IngestionCoordinator coordinator =
                    createCoordinator(client, 3);

            coordinator.accept("""
                    {
                      "test_marker": "%s",
                      "event": "checkout",
                      "price": 100,
                      "tags": ["payment", "success"]
                    }
                    """.formatted(marker));

            assertEquals(0, countByMarker(client, marker));

            coordinator.accept("""
                    {
                      "test_marker": "%s",
                      "event": "payment",
                      "success": true,
                      "scores": [10, 20.5]
                    }
                    """.formatted(marker));

            assertEquals(0, countByMarker(client, marker));

            coordinator.accept("""
                    {
                      "test_marker": "%s",
                      "event": "order",
                      "email": null,
                      "items": [{"product_id": 101}]
                    }
                    """.formatted(marker));

            // Bản ghi thứ ba làm đầy batch và kích hoạt INSERT.
            assertEquals(3, countByMarker(client, marker));

            // Flush thêm không được ghi trùng dữ liệu.
            coordinator.flush();
            assertEquals(3, countByMarker(client, marker));
        }
    }

    @Test
    void shouldFlushPartiallyFilledBatch() throws Exception {
        String marker = UUID.randomUUID().toString();

        try (Client client = createClient()) {
            IngestionCoordinator coordinator =
                    createCoordinator(client, 10);

            coordinator.accept("""
                    {"test_marker":"%s","event":"first"}
                    """.formatted(marker));

            coordinator.accept("""
                    {"test_marker":"%s","event":"second"}
                    """.formatted(marker));

            assertEquals(0, countByMarker(client, marker));

            coordinator.flush();

            assertEquals(2, countByMarker(client, marker));

            // Không được INSERT lại batch đã ghi thành công.
            coordinator.flush();

            assertEquals(2, countByMarker(client, marker));
        }
    }

    @Test
    void shouldAllowFlushWithNoRecords() throws Exception {
        try (Client client = createClient()) {
            IngestionCoordinator coordinator =
                    createCoordinator(client, 3);

            coordinator.flush();
            coordinator.flush();
        }
    }
}
