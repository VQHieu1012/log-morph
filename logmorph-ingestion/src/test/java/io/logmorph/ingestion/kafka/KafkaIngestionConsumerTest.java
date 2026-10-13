package io.logmorph.ingestion.kafka;

import com.clickhouse.client.api.Client;
import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.parser.JacksonLogParser;
import io.logmorph.ingestion.sink.ClickHouseBatchWriter;
import io.logmorph.ingestion.sink.IngestionCoordinator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaIngestionConsumerTest {
    private static final String TABLE = "logmorph.logs";
    private static final String TOPIC = "logmorph-test";

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

    private MockConsumer<String, String> createConsumer(
            TopicPartition partition
    ) {
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        consumer.assign(List.of(partition));
        consumer.updateBeginningOffsets(Map.of(partition, 0L));

        return consumer;
    }

    private long countByMarker(Client client, String marker) {
        String query = """
                SELECT count() as total
                FROM logmorph.logs
                WHERE has(string_values, '%s')
                """.formatted(marker);

        return client.queryAll(query)
                .get(0)
                .getLong("total");
    }

    @Test
    void shouldCommitOnlyAfterSuccessfulFlush() throws Exception {
        String marker = UUID.randomUUID().toString();
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        try (
                Client client = createClient();
                MockConsumer<String, String> consumer = createConsumer(partition);
        ) {
            IngestionCoordinator coordinator = createCoordinator(client, 2);
            KafkaIngestionConsumer ingestion = new KafkaIngestionConsumer(
                    consumer,
                    coordinator,
                    Duration.ofMillis(100)
            );

            for (int i = 0; i < 3; i++) {
                String json = """
                        {
                            "test_marker": "%s",
                            "sequence": %d,
                            "event": "kafka_ingestion"
                        }
                        """.formatted(marker, i);

                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, null, json));
            }

            assertNull(consumer.committed(Set.of(partition)).get(partition));
            ingestion.pollOnce();

            // Batch size = 2:
            // - 2 records đầu kích hoạt automatic flush.
            // - Record cuối được ghi bằng explicit flush.
            assertEquals(3, countByMarker(client, marker));

            var committed = consumer.committed(Set.of(partition)).get(partition);
            assertNotNull(committed);

            assertEquals(3L, committed.offset());
        }
    }

    @Test
    void shouldNotCommitWhenParsingFails() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        try (
                Client client = createClient();
                MockConsumer<String, String> consumer = createConsumer(partition)
        ) {
            IngestionCoordinator coordinator = createCoordinator(client, 10);

            KafkaIngestionConsumer ingestion =
                    new KafkaIngestionConsumer(
                            consumer,
                            coordinator,
                            Duration.ofMillis(100)
                    );

            consumer.addRecord(
                    new ConsumerRecord<>(
                            TOPIC,
                            0,
                            0L,
                            null,
                            "{\"event\":\"valid\"}"
                    )
            );

            consumer.addRecord(
                    new ConsumerRecord<>(
                            TOPIC,
                            0,
                            1L,
                            null,
                            "{\"event\":"
                    )
            );

            assertThrows(Exception.class, ingestion::pollOnce);

            assertNull(
                    consumer.committed(Set.of(partition))
                            .get(partition)
            );
        }
    }

    @Test
    void shouldNotCommitWhenPollIsEmpty() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        try (
                Client client = createClient();
                MockConsumer<String, String> consumer = createConsumer(partition)
        ) {
            IngestionCoordinator coordinator = createCoordinator(client, 10);

            KafkaIngestionConsumer ingestion =
                    new KafkaIngestionConsumer(
                            consumer,
                            coordinator,
                            Duration.ofMillis(100)
                    );

            ingestion.pollOnce();

            assertNull(
                    consumer.committed(Set.of(partition)).get(partition)
            );
        }
    }

    @Test
    void shouldNotCommitWhenClickhouseInsertFails() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        String tableName = "logmorph.kafka_failure";

        try (
                Client client = createClient();
                MockConsumer<String, String> consumer = createConsumer(partition);
        ) {
            try (var response = client.query("CREATE TABLE " + tableName + " AS logmorph.logs").get()) {
                // DDL completed
            }

            try {
                // Coordinator đọc schema khi bảng vẫn tồn tại
                ClickHouseBatchWriter writer = new ClickHouseBatchWriter(client, tableName);

                IngestionCoordinator coordinator = new IngestionCoordinator(
                        new JacksonLogParser(),
                        new BatchAccumulator(10),
                        writer
                );

                KafkaIngestionConsumer ingestion = new KafkaIngestionConsumer(
                        consumer, coordinator, Duration.ofMillis(100)
                );

                try (var response = client.query("DROP TABLE " + tableName).get()) {
                    // DDL completed
                }

                consumer.addRecord(
                        new ConsumerRecord<>(
                                TOPIC, 0, 0L, null, "{\"event\":\"checkout\"}"
                        )
                );

                consumer.addRecord(
                        new ConsumerRecord<>(
                                TOPIC, 0, 1L, null, "{\"event\":\"payment\"}"
                        )
                );

                // flush phải fail vì bảng đã xóa
                assertThrows(Exception.class, ingestion::pollOnce);

                assertNull(
                        consumer.committed(Set.of(partition))
                                .get(partition)
                );
            } finally {
                try (var response = client.query(
                        "DROP TABLE IF EXISTS " + tableName
                ).get()) {
                    // cleanup
                }
            }
        }

    }
}
