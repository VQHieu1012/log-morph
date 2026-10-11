package io.logmorph.ingestion.batch;

import io.logmorph.model.TypedLog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class BatchAccumulatorTest {

    public static TypedLog log(String value) {
        return new TypedLog(
                List.of("event"),
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
                "{\"event\":\"" + value + "\"}"
        );
    }

    @Test
    void shouldReportFullBatch() {
        BatchAccumulator accumulator = new BatchAccumulator(2);

        accumulator.add(log("first"));
        accumulator.add(log("second"));

        assertEquals(2, accumulator.size());
        assertTrue(accumulator.isFull());
    }

    @Test
    void shouldRejectAddingWhenFull() {
        BatchAccumulator accumulator = new BatchAccumulator(1);
        accumulator.add(log("first"));

        assertThrows(
                IllegalStateException.class, () -> accumulator.add(log("second"))
        );

        assertEquals(1, accumulator.size());
    }

    @Test
    void shouldDrainAndResetAccumulator() {
        BatchAccumulator accumulator = new BatchAccumulator(2);
        TypedLog first = log("first");
        TypedLog second = log("second");

        accumulator.add(first);
        accumulator.add(second);

        List<TypedLog> batch = accumulator.drain();

        assertAll(
                () -> assertEquals(List.of(first, second), batch),
                () -> assertEquals(0, accumulator.size()),
                () -> assertFalse(accumulator.isFull())
        );

        accumulator.add(log("third"));

        assertEquals(List.of(first, second), batch);
        assertEquals(1, accumulator.size());
    }

    @Test
    void shouldDrainEmptyAccumulator() {
        BatchAccumulator accumulator = new BatchAccumulator(3);
        List<TypedLog> batch = accumulator.drain();

        assertTrue(batch.isEmpty());
        assertEquals(0, accumulator.size());
    }

    @Test
    void shouldRejectInvalidBatchSize() {
        assertAll(
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> new BatchAccumulator(0)
                ),
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> new BatchAccumulator(-1)
                )
        );
    }

    @Test
    void shouldRejectNullRecord() {
        BatchAccumulator accumulator = new BatchAccumulator(3);

        assertThrows(
                NullPointerException.class,
                () -> accumulator.add(null)
        );

        assertEquals(0, accumulator.size());
    }

    @Test
    void shouldPreserveOriginalRecordReferences() {
        BatchAccumulator accumulator = new BatchAccumulator(2);

        TypedLog first = log("first");

        accumulator.add(first);

        List<TypedLog> batch = accumulator.drain();

        assertSame(first, batch.get(0));
    }
}

