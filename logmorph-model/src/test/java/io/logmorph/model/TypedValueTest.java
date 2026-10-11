package io.logmorph.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TypedLogTest {

    @Test
    void shouldCreateTypedLogWithAllTypes() {
        String rawSource = """
                {
                  "event": "checkout",
                  "price": 100,
                  "success": true,
                  "email": null,
                  "tags": ["payment", "success"],
                  "scores": [10, 20],
                  "flags": [true, false],
                  "items": [{"product_id": 101}]
                }
                """;

        TypedLog log = new TypedLog(List.of("event"), List.of("checkout"),

                List.of("price"), List.of(100.0),

                List.of("success"), List.of(true),

                List.of("email"),

                List.of("tags"), List.of(List.of("payment", "success")),

                List.of("scores"), List.of(List.of(10.0, 20.0)),

                List.of("flags"), List.of(List.of(true, false)),

                List.of("items"), List.of("[{\"product_id\":101}]"),

                rawSource);

        assertAll(() -> assertEquals(List.of("event"), log.stringNames()), () -> assertEquals(List.of("checkout"), log.stringValues()), () -> assertEquals(List.of("price"), log.numberNames()), () -> assertEquals(List.of(100.0), log.numberValues()), () -> assertEquals(List.of("success"), log.booleanNames()), () -> assertEquals(List.of(true), log.booleanValues()), () -> assertEquals(List.of("email"), log.nullNames()), () -> assertEquals(List.of("tags"), log.stringArrayNames()), () -> assertEquals(List.of(List.of("payment", "success")), log.stringArrayValues()), () -> assertEquals(List.of("scores"), log.numberArrayNames()), () -> assertEquals(List.of(List.of(10.0, 20.0)), log.numberArrayValues()), () -> assertEquals(List.of("flags"), log.booleanArrayNames()), () -> assertEquals(List.of(List.of(true, false)), log.booleanArrayValues()), () -> assertEquals(List.of("items"), log.jsonNames()), () -> assertEquals(List.of("[{\"product_id\":101}]"), log.jsonValues()), () -> assertEquals(rawSource, log.rawSource()));
    }

    @ParameterizedTest(name = "Reject mismatched group {0}")
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6})
    void shouldRejectMismatchedNamesAndValues(int group) {
        List<String> stringNames = group == 0 ? List.of("field") : List.of();

        List<String> numberNames = group == 1 ? List.of("field") : List.of();

        List<String> booleanNames = group == 2 ? List.of("field") : List.of();

        List<String> stringArrayNames = group == 3 ? List.of("field") : List.of();

        List<String> numberArrayNames = group == 4 ? List.of("field") : List.of();

        List<String> booleanArrayNames = group == 5 ? List.of("field") : List.of();

        List<String> jsonNames = group == 6 ? List.of("field") : List.of();

        assertThrows(IllegalArgumentException.class, () -> new TypedLog(stringNames, List.of(),

                numberNames, List.of(),

                booleanNames, List.of(),

                List.of(),

                stringArrayNames, List.of(),

                numberArrayNames, List.of(),

                booleanArrayNames, List.of(),

                jsonNames, List.of(),

                "{}"));
    }

    @Test
    void shouldAllowDuplicateFieldNames() {
        TypedLog log = new TypedLog(List.of("extra.age"), List.of("32"),

                List.of("extra.age", "extra.age"), List.of(32.0, 64.0),

                List.of(), List.of(),

                List.of(),

                List.of(), List.of(),

                List.of(), List.of(),

                List.of(), List.of(),

                List.of(), List.of(),

                "{}");

        assertAll(() -> assertEquals(List.of("extra.age"), log.stringNames()), () -> assertEquals(List.of("32"), log.stringValues()), () -> assertEquals(List.of("extra.age", "extra.age"), log.numberNames()), () -> assertEquals(List.of(32.0, 64.0), log.numberValues()));
    }
}