package io.logmorph.catalog;

import io.logmorph.model.TypedLog;
import io.logmorph.model.ValueType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class CatalogBatchWorkerTest {

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

        CountDownLatch persisted = new CountDownLatch(1);

        AtomicReference<Map<String, Set<ValueType>>> actualSchema =
                new AtomicReference<>();

        AtomicReference<Instant> actualObservedAt =
                new AtomicReference<>();

        CatalogRepository repository = (schema, observedAt) -> {
            actualSchema.set(schema);
            actualObservedAt.set(observedAt);
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

            assertTrue(worker.submit(batch));

            assertTrue(
                    persisted.await(5, TimeUnit.SECONDS),
                    "Catalog worker did not persist the batch"
            );

            assertEquals(
                    Map.of(
                            "service", Set.of(ValueType.STRING),
                            "status", Set.of(ValueType.STRING)
                    ),
                    actualSchema.get()
            );

            assertNotNull(actualObservedAt.get());
            assertEquals(0, worker.droppedBatches());
            assertEquals(0, worker.persistFailures());
        }
    }

    @Test
    void shouldContinueWhenRepositoryFails() throws Exception {
        CountDownLatch attempted = new CountDownLatch(1);

        CatalogRepository repository = (schema, observedAt) -> {
            attempted.countDown();
            throw new IllegalStateException("Simulated catalog failure");
        };

        try (CatalogBatchWorker worker = new CatalogBatchWorker(
                new SchemaObserver(),
                repository,
                4
        )) {
            assertTrue(worker.submit(
                    List.of(createLog("service", "payment"))
            ));

            assertTrue(attempted.await(5, TimeUnit.SECONDS));

            // Worker phải ghi nhận lỗi và tiếp tục tồn tại.
            // close() sẽ đợi hoàn tất xử lý queue.
        }
    }
}
