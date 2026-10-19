
package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void shouldAggregateAndPersistBatchAsynchronously()
            throws Exception {

        Instant observedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        CountDownLatch persisted = new CountDownLatch(1);

        AtomicReference<Map<String, Map<ValueType, Instant>>> actualSchema =
                new AtomicReference<>();

        CatalogRepository repository = schema -> {
            actualSchema.set(schema);
            persisted.countDown();
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4
        )) {
            List<TypedLog> batch = List.of(
                    createLog("service", "payment"),
                    createLog("service", "checkout"),
                    createLog("status", "ok")
            );

            assertTrue(worker.submit(batch, observedAt));

            assertTrue(
                    persisted.await(5, TimeUnit.SECONDS),
                    "Catalog worker did not persist the batch"
            );

            assertEquals(
                    Map.of(
                            "service", Map.of(
                                    ValueType.STRING, observedAt
                            ),
                            "status", Map.of(
                                    ValueType.STRING, observedAt
                            )
                    ),
                    actualSchema.get()
            );

            assertEquals(0, worker.droppedBatches());
            assertEquals(0, worker.persistFailures());
        }
    }

    @Test
    void shouldContinueWhenRepositoryFails() throws Exception {
        CountDownLatch attempted = new CountDownLatch(1);

        CatalogRepository repository = schema -> {
            attempted.countDown();
            throw new IllegalStateException(
                    "Simulated catalog failure"
            );
        };

        Instant observedAt =
                Instant.parse("2026-10-19T10:00:00Z");

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4
        )) {
            assertTrue(worker.submit(
                    List.of(createLog("service", "payment")),
                    observedAt
            ));

            assertTrue(attempted.await(5, TimeUnit.SECONDS));
        }

        // close() đợi worker hoàn tất xử lý queue.
        // Lỗi repository không được propagate sang ingestion thread.
    }

    @Test
    void shouldPreserveObservedAtFromSuccessfulBatch()
            throws Exception {

        Instant batchObservedAt =
                Instant.parse("2026-10-19T09:30:00Z");

        CountDownLatch persisted = new CountDownLatch(1);

        AtomicReference<Map<String, Map<ValueType, Instant>>> actualSchema =
                new AtomicReference<>();

        CatalogRepository repository = schema -> {
            actualSchema.set(schema);
            persisted.countDown();
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4
        )) {
            assertTrue(worker.submit(
                    List.of(createLog("status", "success")),
                    batchObservedAt
            ));

            assertTrue(
                    persisted.await(5, TimeUnit.SECONDS)
            );

            assertEquals(
                    batchObservedAt,
                    actualSchema.get()
                            .get("status")
                            .get(ValueType.STRING)
            );
        }
    }
}
