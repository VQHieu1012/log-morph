package io.logmorph.ingestion.kafka;

import io.logmorph.ingestion.sink.IngestionCoordinator;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public final class KafkaIngestionConsumer {
    private final Consumer<String, String> consumer;
    private final IngestionCoordinator coordinator;
    private final Duration pollTimeout;

    public KafkaIngestionConsumer (
            Consumer<String, String> consumer,
            IngestionCoordinator coordinator,
            Duration pollTimeout
    ) {
        this.consumer = Objects.requireNonNull(consumer);
        this.coordinator = Objects.requireNonNull(coordinator);
        this.pollTimeout = Objects.requireNonNull(pollTimeout);
    }

    public void pollOnce() throws ExecutionException, InterruptedException, IOException {
        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);

        if (records.isEmpty()) {
            return;
        }

        for (var record : records) {
            coordinator.accept(record.value());
        }

        coordinator.flush();
        consumer.commitSync();
    }

    public void run()
            throws InterruptedException, ExecutionException, IOException {

        while (!Thread.currentThread().isInterrupted()) {
            pollOnce();
        }
    }
}
