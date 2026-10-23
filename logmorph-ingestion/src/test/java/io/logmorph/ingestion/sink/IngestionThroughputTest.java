package io.logmorph.ingestion.sink;

import com.clickhouse.client.api.Client;
import io.logmorph.catalog.CatalogBatchWorker;
import io.logmorph.catalog.PostgresCatalogRepository;
import io.logmorph.catalog.SchemaObserver;
import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.parser.JacksonLogParser;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class IngestionThroughputTest {

    private static final String TABLE = "logmorph.logs";

    private static final int MESSAGE_COUNT = 1_000_000;
    private static final int BATCH_SIZE = 5_000;
    private static final int WARMUP_COUNT = 2_000;

    private static final int FIELD_CARDINALITY =
            Integer.getInteger("logmorph.bench.cardinality", 1);

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
        int fieldIndex = sequence % FIELD_CARDINALITY;

        return """
                {
                  "bench_marker": "%s",
                  "sequence": %d,
                  "timestamp": "2026-10-23T10:00:00Z",
                  "service": "payment",
                  "environment": "production",
                  "level": "INFO",
                  "event": "checkout",
                  "request_id": "req-%d",
                  "trace_id": "trace-%d",
                  "host": "payment-worker-01",
                  "region": "ap-southeast-1",
                  "http_method": "POST",
                  "http_status": 200,
                  "duration_ms": 125.75,
                  "success": true,
                  "retry_count": 0,
                  "dynamic_field_%d": 100,
                  "tags": ["api", "payment", "production"],
                  "metadata": {
                    "endpoint": "/api/v1/checkout",
                    "version": "v1"
                  }
                }
                """.formatted(
                marker,
                sequence,
                sequence,
                sequence,
                fieldIndex
        );
    }


    private CatalogBatchWorker createCatalogWorker() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setURL(requireEnv("POSTGRES_URL"));
        dataSource.setUser(requireEnv("POSTGRES_USER"));
        dataSource.setPassword(requireEnv("POSTGRES_PASSWORD"));

        return new CatalogBatchWorker(
                new SchemaObserver(),
                new PostgresCatalogRepository(dataSource),
                16,
                100,
                java.time.Duration.ofSeconds(30)
        );
    }

    private String requireEnv(String name) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing environment variable: " + name
            );
        }

        return value;
    }

    private long countByMarker(Client client, String marker) {
        String sql = """
                SELECT count() AS total
                FROM logmorph.logs
                WHERE has(string_values, '%s')
                """.formatted(marker);

        return client.queryAll(sql)
                .get(0)
                .getLong("total");
    }

    @Test
    void benchmark100kMessages() throws Exception {
        assumeTrue(
                Boolean.getBoolean("logmorph.bench"),
                "Enable with -Dlogmorph.bench=true"
        );

        boolean catalogEnabled = Boolean.getBoolean(
                "logmorph.bench.catalog"
        );

        String warmupMarker = UUID.randomUUID().toString();
        String benchmarkMarker = UUID.randomUUID().toString();

        try (Client client = createClient()) {
            ClickHouseBatchWriter writer =
                    new ClickHouseBatchWriter(client, TABLE);

            // Warm-up: not included in the benchmark.
            IngestionCoordinator warmup = new IngestionCoordinator(
                    new JacksonLogParser(),
                    new BatchAccumulator(BATCH_SIZE),
                    writer
            );

            for (int i = 0; i < WARMUP_COUNT; i++) {
                warmup.accept(generateMessage(warmupMarker, i));
            }

            warmup.flush();

            CatalogBatchWorker catalogWorker =
                    catalogEnabled ? createCatalogWorker() : null;

            long totalPayloadBytes = 0;
            long elapsedNanos;
            long catalogDrainNanos = 0;

            long droppedBatches = 0;
            long persistFailures = 0;

            try {
                IngestionCoordinator coordinator =
                        new IngestionCoordinator(
                                new JacksonLogParser(),
                                new BatchAccumulator(BATCH_SIZE),
                                writer,
                                catalogWorker
                        );

                long start = System.nanoTime();

                for (int i = 0; i < MESSAGE_COUNT; i++) {
                    String json = generateMessage(
                            benchmarkMarker,
                            i
                    );

                    totalPayloadBytes += json.getBytes(
                            StandardCharsets.UTF_8
                    ).length;

                    coordinator.accept(json);
                }

                coordinator.flush();

                elapsedNanos = System.nanoTime() - start;

            } finally {
                if (catalogWorker != null) {
                    long drainStart = System.nanoTime();

                    catalogWorker.close();

                    catalogDrainNanos =
                            System.nanoTime() - drainStart;

                    droppedBatches = catalogWorker.droppedBatches();
                    persistFailures = catalogWorker.persistFailures();
                }
            }

            // Verification is excluded from throughput measurement.
            long actualRows = countByMarker(
                    client,
                    benchmarkMarker
            );

            assertEquals(MESSAGE_COUNT, actualRows);

            double seconds = elapsedNanos / 1_000_000_000.0;

            double throughput = MESSAGE_COUNT / seconds;

            double payloadMiB =
                    totalPayloadBytes / (1024.0 * 1024.0);

            double payloadThroughput = payloadMiB / seconds;

            System.out.printf(
                    Locale.ROOT,
                    """
                            
                            ========= LOGMORPH BENCHMARK =========
                            Catalog enabled  : %s
                            Messages         : %,d
                            Batch size       : %,d
                            Field cardinality: %,d
                            INSERT count     : %,d
                            Total JSON       : %.2f MiB
                            Elapsed time     : %.3f seconds
                            Throughput       : %,.0f messages/sec
                            JSON throughput  : %.2f MiB/sec
                            Average latency  : %.3f us/message
                            Verified rows    : %,d
                            
                            Catalog drain    : %.3f seconds
                            Dropped batches  : %,d
                            Persist failures : %,d
                            ======================================
                            
                            """,
                    catalogEnabled,
                    MESSAGE_COUNT,
                    BATCH_SIZE,
                    FIELD_CARDINALITY,
                    (MESSAGE_COUNT + BATCH_SIZE - 1) / BATCH_SIZE,
                    payloadMiB,
                    seconds,
                    throughput,
                    payloadThroughput,
                    seconds * 1_000_000 / MESSAGE_COUNT,
                    actualRows,
                    catalogDrainNanos / 1_000_000_000.0,
                    droppedBatches,
                    persistFailures
            );

            if (catalogEnabled) {
                assertEquals(
                        0,
                        persistFailures,
                        "Catalog persistence failed during benchmark"
                );
            }
        }
    }
}
