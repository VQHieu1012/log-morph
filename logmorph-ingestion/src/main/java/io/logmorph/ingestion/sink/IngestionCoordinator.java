package io.logmorph.ingestion.sink;

import io.logmorph.ingestion.batch.BatchAccumulator;
import io.logmorph.ingestion.parser.JacksonLogParser;
import io.logmorph.model.TypedLog;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public final class IngestionCoordinator {
    private final JacksonLogParser parser;
    private final BatchAccumulator accumulator;
    private final ClickHouseBatchWriter writer;

    private List<TypedLog> pendingBatch;

    public IngestionCoordinator(
            JacksonLogParser parser,
            BatchAccumulator accumulator,
            ClickHouseBatchWriter writer
    ) {
        this.parser = Objects.requireNonNull(parser);
        this.accumulator = Objects.requireNonNull(accumulator);
        this.writer = Objects.requireNonNull(writer);
    }

    public void accept(String json) throws InterruptedException, ExecutionException, IOException {
        if (pendingBatch != null) {
            throw new IllegalStateException(
                    "Cannot accept records while a batch is pending"
            );
        }

        TypedLog log = parser.parse(json);

        accumulator.add(log);

        if (accumulator.isFull()) flush();
    }

    public void flush() throws ExecutionException, InterruptedException {
        if (pendingBatch == null) {
            if (accumulator.size() == 0) {
                return;
            }

            pendingBatch = accumulator.drain();
        }

        writer.write(pendingBatch);

        pendingBatch = null;
    }
}
