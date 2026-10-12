package io.logmorph.ingestion.sink;

import com.clickhouse.client.api.Client;
import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.parser.JacksonLogParser;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class IngestionThroughputTest {
    private static final String TABLE = "logmorph.logs";
    private static final int MESSAGE_COUNT = 100_000;
    private static final int BATCH_SIZE = 10_000;
    private static final int WARMUP_COUNT = 2_000;

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

    private String generateMessage(String marker, int sequence) {
        return """
                {
                  "bench_marker": "%s",
                  "sequence": %d,
                  "service": "payment",
                  "event": "checkout",
                  "amount": 125.75,
                  "success": true,
                  "tags": ["api", "payment", "production"],
                  "scores": [10.5, 20.25, 30.75],
                  "metadata": {
                    "region": "ap-southeast-1",
                    "host": "worker-01"
                  }
                }
                """.formatted(marker, sequence);
    }

    private IngestionCoordinator createCoordinator(
            ClickHouseBatchWriter writer
    ) {
        return new IngestionCoordinator(
                new JacksonLogParser(),
                new BatchAccumulator(BATCH_SIZE),
                writer
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
    void benchmark100kMessages() throws Exception {
        assumeTrue(
                Boolean.getBoolean("logmorph.bench"),
                "Enable with -Dlogmorph.bench=true"
        );

        String warmupMarker = UUID.randomUUID().toString();
        String benchmarkMarker = UUID.randomUUID().toString();

        try (Client client = createClient()) {
            ClickHouseBatchWriter writer =
                    new ClickHouseBatchWriter(client, TABLE);

            // Warm-up: không tính vào throughput.
            IngestionCoordinator warmup = createCoordinator(writer);

            for (int i = 0; i < WARMUP_COUNT; i++) {
                warmup.accept(generateMessage(warmupMarker, i));
            }

            warmup.flush();

            // Benchmark thực tế.
            IngestionCoordinator coordinator = createCoordinator(writer);

            long totalPayloadBytes = 0;

            long start = System.nanoTime();

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String json = generateMessage(benchmarkMarker, i);

                totalPayloadBytes +=
                        json.getBytes(StandardCharsets.UTF_8).length;

                coordinator.accept(json);
            }

            coordinator.flush();

            long elapsedNanos = System.nanoTime() - start;
            double seconds = elapsedNanos / 1_000_000_000.0;

            double throughput = MESSAGE_COUNT / seconds;
            double payloadMiB =
                    totalPayloadBytes / (1024.0 * 1024.0);
            double payloadThroughput = payloadMiB / seconds;

            // Kiểm tra sau khi kết thúc phép đo.
            long actualRows =
                    countByMarker(client, benchmarkMarker);

            assertEquals(MESSAGE_COUNT, actualRows);

            System.out.printf(
                    Locale.ROOT,
                    """
                            
                            ========= LOGMORPH THROUGHPUT =========
                            Messages        : %,d
                            Batch size      : %,d
                            INSERT count    : %,d
                            Total JSON      : %.2f MiB
                            Elapsed time    : %.3f seconds
                            Throughput      : %,.0f messages/sec
                            JSON throughput : %.2f MiB/sec
                            Average latency : %.3f us/message
                            Verified rows   : %,d
                            =======================================
                            
                            """,
                    MESSAGE_COUNT,
                    BATCH_SIZE,
                    (MESSAGE_COUNT + BATCH_SIZE - 1) / BATCH_SIZE,
                    payloadMiB,
                    seconds,
                    throughput,
                    payloadThroughput,
                    seconds * 1_000_000 / MESSAGE_COUNT,
                    actualRows
            );
        }
    }
}
