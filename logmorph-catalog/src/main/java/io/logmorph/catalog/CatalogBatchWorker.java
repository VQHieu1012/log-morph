package io.logmorph.catalog;

import io.logmorph.model.TypedLog;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class CatalogBatchWorker implements AutoCloseable {
    private final BlockingQueue<List<TypedLog>> queue;
    private final SchemaObserver observer;
    private final CatalogRepository repository;
    private final Thread workerThread;

    private final AtomicLong droppedBatches = new AtomicLong();
    private final AtomicLong persistFailures = new AtomicLong();

    private volatile boolean stopping;

    public CatalogBatchWorker(
            SchemaObserver observer,
            CatalogRepository repository,
            int queueCapacity
    ) {
        if (queueCapacity <= 0) throw new IllegalArgumentException(
                "queueCapacity must be positive"
        );
        this.observer = observer;
        this.repository = repository;

        this.queue = new ArrayBlockingQueue<>(queueCapacity);

        this.workerThread = new Thread(this::runLoop, "logmorph-catalog-worker");

        this.workerThread.start();
    }

    public boolean submit(List<TypedLog> batch) {
        Objects.requireNonNull(batch);

        if (batch.isEmpty()) {
            return true;
        }

        if (stopping || !queue.offer(batch)) {
            droppedBatches.incrementAndGet();
            return false;
        }

        return true;
    }

    private void runLoop() {
        try {
            while (!stopping || !queue.isEmpty()) {
                List<TypedLog> batch = queue.poll(200, TimeUnit.MILLISECONDS);

                if (batch == null) {
                    continue;
                }

                process(batch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(List<TypedLog> batch) {
        try {
            SchemaAggregator aggregator = new SchemaAggregator();

            for (TypedLog log : batch) {
                aggregator.add(observer.observe(log));
            }

            var schema = aggregator.snapshot();

            if (!schema.isEmpty()) {
                repository.persist(schema, Instant.now());
            }
        } catch (Exception e) {
            persistFailures.incrementAndGet();

            System.err.println(
                    "Catalog batch processing failed: " + e
            );
        }
    }

    public int queueSize() {
        return queue.size();
    }

    public long droppedBatches() {
        return droppedBatches.get();
    }

    public long persistFailures() {
        return persistFailures.get();
    }

    @Override
    public void close() throws InterruptedException {
        stopping = true;
        workerThread.join();
    }
}
