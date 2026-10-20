package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CatalogBatchWorkerTest {

    private TypedLog createLog(String field, String value) {
        return new TypedLog(
                List.of(field),
                List.of(value),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                List.of(),
                List.of(),

                "{}"
        );
    }


    @Test
    void shouldFlushWhenBatchThresholdReached() throws Exception {
        CountDownLatch flushed = new CountDownLatch(1);
        AtomicInteger persistCalls = new AtomicInteger();

        CatalogRepository repository = schema -> {
            persistCalls.incrementAndGet();
            flushed.countDown();
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                2,
                Duration.ofSeconds(30)
        )) {
            Instant t = Instant.parse("2026-10-19T10:00:00Z");

            assertTrue(worker.submit(
                    List.of(createLog("service", "payment")), t
            ));

            assertTrue(worker.submit(
                    List.of(createLog("status", "ok")), t
            ));

            assertTrue(flushed.await(5, TimeUnit.SECONDS));
            assertEquals(1, persistCalls.get());
        }
    }

    @Test
    void shouldFlushWhenIntervalElapsedWithoutNewBatches()
            throws Exception {

        CountDownLatch flushed = new CountDownLatch(1);

        CatalogRepository repository = schema ->
                flushed.countDown();

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                100,
                Duration.ofMillis(300)
        )) {
            assertTrue(worker.submit(
                    List.of(createLog("service", "payment")),
                    Instant.parse("2026-10-19T10:00:00Z")
            ));

            assertTrue(
                    flushed.await(5, TimeUnit.SECONDS),
                    "Time-based flush did not execute"
            );
        }
    }

    @Test
    void shouldRetainSchemaAcrossSuccessfulFlushes()
            throws Exception {

        CountDownLatch flushed = new CountDownLatch(2);

        AtomicReference<Map<String, Map<ValueType, Instant>>> lastSchema =
                new AtomicReference<>();

        CatalogRepository repository = schema -> {
            lastSchema.set(schema);
            flushed.countDown();
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                1,
                Duration.ofSeconds(30)
        )) {
            Instant t1 = Instant.parse("2026-10-19T10:00:00Z");
            Instant t2 = Instant.parse("2026-10-19T10:05:00Z");

            assertTrue(worker.submit(
                    List.of(createLog("service", "payment")), t1
            ));

            assertTrue(worker.submit(
                    List.of(createLog("status", "ok")), t2
            ));

            assertTrue(flushed.await(5, TimeUnit.SECONDS));

            assertEquals(
                    Map.of(
                            "service", Map.of(ValueType.STRING, t1),
                            "status", Map.of(ValueType.STRING, t2)
                    ),
                    lastSchema.get()
            );
        }
    }

    @Test
    void shouldRetryFailedPersistenceAfterFlushInterval() {
        Instant observedAt = Instant.parse("2026-10-20T10:00:00Z");

        AtomicInteger persistAttempts = new AtomicInteger();

        CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        CountDownLatch retrySucceeded = new CountDownLatch(1);

        AtomicReference<Map<String, Map<ValueType, Instant>>> persistedSchema = new AtomicReference<>();

        CatalogRepository repository = schema -> {
            int attempt = persistAttempts.incrementAndGet();

            if (attempt == 1) {
                firstAttemptFailed.countDown();
                throw new SQLException("Simulated PostgeSQL failure");
            }

            persistedSchema.set(schema);
            retrySucceeded.countDown();
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                1,
                Duration.ofMillis(300)
        )) {
            assertTrue(worker.submit(
                    List.of(createLog("status", "ok")),
                    observedAt
            ));

            assertTrue(
                    firstAttemptFailed.await(5, TimeUnit.SECONDS),
                    "First persistence attempt did not execute"
            );

            assertTrue(
                    retrySucceeded.await(5, TimeUnit.SECONDS),
                    "Worker did not retry persistence"
            );

            assertEquals(
                    Map.of(
                            "status",
                            Map.of(ValueType.STRING, observedAt)
                    ),
                    persistedSchema.get()
            );

            assertEquals(2, persistAttempts.get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldFlushPendingSchemaOnClose() throws Exception {
        Instant observedAt = Instant.parse("2026-10-20T10:00:00Z");

        AtomicInteger persistCalls = new AtomicInteger();

        AtomicReference<Map<String, Map<ValueType, Instant>>> persistedSchema =
                new AtomicReference<>();

        CatalogRepository repository = schema -> {
            persistedSchema.set(schema);
            persistCalls.incrementAndGet();
        };

        CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                100,
                Duration.ofSeconds(30)
        );

        assertTrue(worker.submit(
                List.of(createLog("status", "ok")),
                observedAt
        ));

        worker.close();

        assertEquals(1, persistCalls.get());

        assertEquals(
                Map.of(
                        "status",
                        Map.of(ValueType.STRING, observedAt)
                ),
                persistedSchema.get()
        );
    }

    @Test
    void shouldRejectSubmissionAfterClose() throws Exception {
        AtomicInteger persistCalls = new AtomicInteger();

        CatalogRepository repository;
        repository = schema -> {
            persistCalls.incrementAndGet();
        };

        CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                100,
                Duration.ofSeconds(30)
        );

        worker.close();

        boolean accepted = worker.submit(
                List.of(createLog("status", "ok")),
                Instant.parse("2026-10-20T10:00:00Z")
        );

        assertFalse(accepted);
        assertEquals(0, persistCalls.get());
        assertEquals(1, worker.droppedBatches());
    }

    @Test
    void shouldNotLoseAcceptedBatchDuringConcurrentClose()
            throws Exception {

        AtomicInteger persistCalls = new AtomicInteger();

        CatalogRepository repository = schema -> {
            persistCalls.incrementAndGet();
        };

        CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4,
                100,
                Duration.ofSeconds(30)
        );

        CountDownLatch start = new CountDownLatch(1);

        AtomicReference<Boolean> accepted = new AtomicReference<>();

        Thread submitThread = new Thread(() -> {
            try {
                start.await();

                accepted.set(worker.submit(
                        List.of(createLog("status", "ok")),
                        Instant.parse("2026-10-20T10:00:00Z")
                ));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread closeThread = new Thread(() -> {
            try {
                start.await();
                worker.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        submitThread.start();
        closeThread.start();

        // Cho phép hai threads bắt đầu cạnh tranh lock.
        start.countDown();

        submitThread.join(5000);
        closeThread.join(5000);

        assertFalse(submitThread.isAlive());
        assertFalse(closeThread.isAlive());

        assertNotNull(accepted.get());

        if (accepted.get()) {
            assertEquals(
                    1,
                    persistCalls.get(),
                    "Accepted batch must be persisted before worker exits"
            );
        } else {
            assertEquals(
                    0,
                    persistCalls.get(),
                    "Rejected batch must not be persisted"
            );
        }
    }
}
