package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
