package io.logmorph.ingestion;

import com.clickhouse.client.api.Client;
import io.logmorph.catalog.CatalogBatchWorker;
import io.logmorph.catalog.PostgresCatalogRepository;
import io.logmorph.catalog.SchemaObserver;
import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.kafka.KafkaIngestionConsumer;
import io.logmorph.ingestion.parser.JacksonLogParser;
import io.logmorph.ingestion.sink.ClickHouseBatchWriter;
import io.logmorph.ingestion.sink.IngestionCoordinator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class LogMorphApplication {
    private static final int BATCH_SIZE = 5_000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1_000);

    private LogMorphApplication() {
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        String bootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String topic = requireEnv("KAFKA_TOPIC");
        String groupId = requireEnv("KAFKA_GROUP_ID");

        String clickHouseUrl = requireEnv("CLICKHOUSE_URL");
        String clickHouseUser = requireEnv("CLICKHOUSE_USER");
        String clickHousePassword = requireEnv("CLICKHOUSE_PASSWORD");

        Properties kafkaProperties = new Properties();

        kafkaProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        kafkaProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        kafkaProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        kafkaProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        kafkaProperties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        kafkaProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        kafkaProperties.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                BATCH_SIZE
        );

        try (
                Client client = new Client.Builder()
                        .addEndpoint(clickHouseUrl)
                        .setUsername(clickHouseUser)
                        .setPassword(clickHousePassword)
                        .compressServerResponse(false)
                        .serverSetting(
                                "http_x_clickhouse_format_overrides_output_format",
                                "0"
                        )
                        .serverSetting("async_insert", "0")
                        .build();
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
                CatalogBatchWorker catalogWorker = createCatalogWorker()
        ) {
            JacksonLogParser parser = new JacksonLogParser();
            BatchAccumulator accumulator = new BatchAccumulator(BATCH_SIZE);

            ClickHouseBatchWriter writer = new ClickHouseBatchWriter(client, "logmorph.logs");

            IngestionCoordinator coordinator = new IngestionCoordinator(
                    parser,
                    accumulator,
                    writer,
                    catalogWorker
            );

            KafkaIngestionConsumer ingestion = new KafkaIngestionConsumer(
                    consumer, coordinator, POLL_TIMEOUT
            );

            consumer.subscribe(List.of(topic));
            ingestion.run();
        }

    }

    public static String requireEnv(String name) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing environment variable: " + name
            );
        }

        return value;
    }

    public static CatalogBatchWorker createCatalogWorker() {
        boolean enabled = Boolean.parseBoolean(
                System.getenv().getOrDefault(
                        "CATALOG_ENABLED", "false"
                )
        );

        if (!enabled) {
            return null;
        }

        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setURL(requireEnv("POSTGRES_URL"));
        dataSource.setUser(requireEnv("POSTGRES_USER"));
        dataSource.setPassword(requireEnv("POSTGRES_PASSWORD"));

        return new CatalogBatchWorker(
                new SchemaObserver(),
                new PostgresCatalogRepository(dataSource),
                4,
                100,
                Duration.ofSeconds(30)
        );
    }
}
