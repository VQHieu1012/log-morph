package io.logmorph.ingestion.kafka;

import io.logmorph.ingestion.sink.IngestionCoordinator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class KafkaIngestionConsumer {

    private static final long REPORT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final Consumer<String, String> consumer;
    private final IngestionCoordinator coordinator;
    private final Duration pollTimeout;

    private long totalCommittedRecords = 0;
    private long lastReportedRecords = 0;

    private long startedAtNanos;
    private long lastReportNanos;

    public KafkaIngestionConsumer(
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

        totalCommittedRecords += records.count();
    }

    public void run()
            throws InterruptedException, ExecutionException, IOException {

        while (!Thread.currentThread().isInterrupted()) {
            pollOnce();
        }
    }

    private void reportIfDue() {
        long now = System.nanoTime();
        long intervalNanos = now - lastReportNanos;

        if (intervalNanos < REPORT_INTERVAL_NANOS) return;

        long intervalRecords = totalCommittedRecords - lastReportedRecords;

        double intervalSeconds = intervalNanos / 1_000_000_000.0;

        double totalSeconds = (now - startedAtNanos) / 1_000_000_000.0;

        double currentThroughput = intervalRecords / intervalSeconds;

        double averageThroughput = totalCommittedRecords / totalSeconds;

        System.out.printf(
                "INGESTION_METRICS " +
                        "committed=%d " +
                        "current_throughput=%.2f logs/s " +
                        "average_throughput=%.2f logs/s%n",
                totalCommittedRecords,
                currentThroughput,
                averageThroughput
        );

        lastReportedRecords = totalCommittedRecords;
        lastReportNanos = now;
    }
}
