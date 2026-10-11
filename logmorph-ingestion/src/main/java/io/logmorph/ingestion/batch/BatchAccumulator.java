package io.logmorph.ingestion.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.logmorph.model.TypedLog;

public final class BatchAccumulator {

    private final int maxBatchSize;
    private List<TypedLog> records;

    public BatchAccumulator(int maxBatchSize) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException(
                    "maxBatchSize must be positive"
            );
        }
        this.maxBatchSize = maxBatchSize;
        this.records = new ArrayList<>(maxBatchSize);
    }

    public void add(TypedLog log) {
        Objects.requireNonNull(log, "log");

        if (isFull()) {
            throw new IllegalStateException(
                    "Batch is full; drain before adding more records"
            );
        }
        records.add(log);
    }

    public boolean isFull() {
        return records.size() >= maxBatchSize;
    }

    public int size() {
        return records.size();
    }

    public List<TypedLog> drain() {
        List<TypedLog> batch = records;
        records = new ArrayList<>(maxBatchSize);

        return batch;
    }
}