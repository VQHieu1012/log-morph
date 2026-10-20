package io.logmorph.catalog;

import io.logmorph.model.TypedLog;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class CatalogBatchWorker implements AutoCloseable {
    private final BlockingQueue<SuccessfulBatch> queue;
    private final SchemaObserver observer;
    private final CatalogRepository repository;
    private final Thread workerThread;

    private final int flushBatchThreshold;
    private final long flushIntervalNanos;

    private final AtomicLong droppedBatches = new AtomicLong();
    private final AtomicLong persistFailures = new AtomicLong();

    private final SchemaAggregator aggregator = new SchemaAggregator();

    private int batchesSinceFlush = 0;
    private boolean dirty = false;
    private long lastFlushAttemptNanos;

    private volatile boolean stopping;

    public CatalogBatchWorker(
            SchemaObserver observer,
            CatalogRepository repository,
            int queueCapacity,
            int flushBatchThreshold,
            Duration flushInterval
    ) {
        if (queueCapacity <= 0) throw new IllegalArgumentException(
                "queueCapacity must be positive"
        );

        if (flushBatchThreshold <= 0) {
            throw new IllegalArgumentException(
                    "flushBatchThreshold must be positive"
            );
        }

        Objects.requireNonNull(flushInterval);

        if (flushInterval.isZero() || flushInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "flushInterval must be positive"
            );
        }

        this.observer = observer;
        this.repository = repository;

        this.queue = new ArrayBlockingQueue<>(queueCapacity);

        this.flushBatchThreshold = flushBatchThreshold;
        this.flushIntervalNanos = flushInterval.toNanos();

        this.lastFlushAttemptNanos = System.nanoTime();

        this.workerThread = new Thread(this::runLoop, "logmorph-catalog-worker");

        this.workerThread.start();
    }

    public synchronized boolean submit(
            List<TypedLog> batch,
            Instant observedAt
    ) {
        Objects.requireNonNull(batch);
        Objects.requireNonNull(observedAt);

        if (batch.isEmpty()) {
            return true;
        }

        SuccessfulBatch successfulBatch =
                new SuccessfulBatch(batch, observedAt);

        if (stopping || !queue.offer(successfulBatch)) {
            droppedBatches.incrementAndGet();
            return false;
        }

        return true;
    }

    private void runLoop() {
        try {
            while (!stopping || !queue.isEmpty()) {
                SuccessfulBatch batch = queue.poll(200, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    process(batch);
                }

                if (shouldFlush()) {
                    flush();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (dirty) {
                flush();
            }
        }
    }

    private void process(SuccessfulBatch batch) {
        for (TypedLog log : batch.logs()) {
            observer.observeInto(log, batch.observedAt(), aggregator);
        }

        batchesSinceFlush++;
        dirty = true;
    }

    private boolean shouldFlush() {
        if (!dirty) {
            return false;
        }
        boolean batchThresholdReached = batchesSinceFlush >= flushBatchThreshold;
        boolean intervalElapsed = System.nanoTime() - lastFlushAttemptNanos >= flushIntervalNanos;

        return batchThresholdReached || intervalElapsed;
    }

    private void flush() {
        lastFlushAttemptNanos = System.nanoTime();
        batchesSinceFlush = 0;

        try {
            repository.persist(aggregator.snapshot());
            dirty = false;
        } catch (Exception e) {
            persistFailures.incrementAndGet();
            dirty = true;
            System.err.println(
                    "Catalog flush failed: " + e
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
        synchronized (this) {
            stopping = true;
        }
        workerThread.join();
    }
}
