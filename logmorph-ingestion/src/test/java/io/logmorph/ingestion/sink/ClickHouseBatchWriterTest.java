package io.logmorph.ingestion.sink;

import com.clickhouse.client.api.Client;
import io.logmorph.ingestion.parser.JacksonLogParser;
import io.logmorph.model.TypedLog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ClickHouseBatchWriterTest {

    private final JacksonLogParser parser = new JacksonLogParser();

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

    @Test
    void shouldWriterMultipleLogsInOneBatch() throws Exception {

        TypedLog first = parser.parse("""
                {
                    "test_marker": "100",
                    "event": "checkout",
                    "price": 100,
                    "tags": ["payment", "success"]
                }
                """);

        TypedLog second = parser.parse("""
                {
                  "test_marker": "101",
                  "event": "payment",
                  "success": true,
                  "scores": [10, 20.5]
                }
                """);

        TypedLog third = parser.parse("""
                {
                  "test_marker": "102",
                  "event": "order",
                  "email": null,
                  "items": [
                    {"product_id": 101, "price": 100},
                    {"product_id": 102, "price": 200}
                  ]
                }
                """);

        try (Client client = createClient()) {
            ClickHouseBatchWriter writer = new ClickHouseBatchWriter(client, "logmorph.logs");
            writer.write(List.of(first, second, third));
        }

        System.out.println("Inserted batch marker");
    }

    @Test
    void shouldIgnoreEmptyBatch() {
        try (Client client = createClient()) {
            ClickHouseBatchWriter writer =
                    new ClickHouseBatchWriter(
                            client,
                            "logmorph.logs"
                    );

            assertDoesNotThrow(
                    () -> writer.write(List.of())
            );
        }
    }

}
