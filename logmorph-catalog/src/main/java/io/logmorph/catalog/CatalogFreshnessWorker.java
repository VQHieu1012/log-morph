
package io.logmorph.catalog;

import io.logmorph.model.ValueType;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class CatalogFreshnessWorker implements AutoCloseable {

    private final CatalogRepository repository;
    private final ExecutorService executor;

    private Map<String, Map<ValueType, Instant>> pendingSnapshot;

    private boolean running;
    private boolean closed;

    public CatalogFreshnessWorker(CatalogRepository repository) {
        this.repository = Objects.requireNonNull(repository);

        this.executor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(
                        runnable,
                        "logmorph-catalog-freshness"
                )
        );
    }

    public synchronized boolean submit(
            Map<String, Map<ValueType, Instant>> snapshot
    ) {
        Objects.requireNonNull(snapshot);

        if (closed) return false;
        if (snapshot.isEmpty()) return true;

        // snapshot được tạo từ SchemaAggregator.snapshot(),
        // nên cả outer map và inner maps đều immutable.
        pendingSnapshot = snapshot;

        if (!running) {
            running = true;
            executor.execute(this::run);
        }

        return true;
    }

    private void run() {
        while (true) {
            Map<String, Map<ValueType, Instant>> snapshot;

            synchronized (this) {
                snapshot = pendingSnapshot;

                if (snapshot == null) {
                    running = false;
                    return;
                }
            }

            try {
                repository.refreshObservedAt(snapshot);
            } catch (Exception e) {
                System.err.println(
                        "Catalog freshness refresh failed: " + e
                );

                synchronized (this) {
                    running = false;
                }

                // Giữ pendingSnapshot để lần submit tiếp theo retry.
                return;
            }

            synchronized (this) {
                if (pendingSnapshot == snapshot) {
                    pendingSnapshot = null;
                }
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        synchronized (this) {
            if (closed) return;

            closed = true;

            if (!running && pendingSnapshot != null) {
                running = true;
                executor.execute(this::run);
            }
            executor.shutdown();
        }

        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}
