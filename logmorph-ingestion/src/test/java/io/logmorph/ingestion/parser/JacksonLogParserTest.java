package io.logmorph.ingestion.parser;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.logmorph.model.TypedLog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JacksonLogParserTest {

    private final JacksonLogParser parser = new JacksonLogParser();

    @Test
    void shouldFlattenNestedObjectsAndClassifyScalars() throws IOException {
        String json = """
                {
                  "event": "checkout",
                  "user": {
                    "id": 101,
                    "profile": {
                      "active": true,
                      "email": null
                    }
                  }
                }
                """;

        TypedLog log = parser.parse(json);

        assertAll(
                () -> assertEquals(
                        List.of("event"),
                        log.stringNames()
                ),
                () -> assertEquals(
                        List.of("checkout"),
                        log.stringValues()
                ),
                () -> assertEquals(
                        List.of("user.id"),
                        log.numberNames()
                ),
                () -> assertEquals(
                        List.of(101.0),
                        log.numberValues()
                ),
                () -> assertEquals(
                        List.of("user.profile.active"),
                        log.booleanNames()
                ),
                () -> assertEquals(
                        List.of(true),
                        log.booleanValues()
                ),
                () -> assertEquals(
                        List.of("user.profile.email"),
                        log.nullNames()
                ),
                () -> assertEquals(json, log.rawSource())
        );
    }

    @Test
    void shouldClassifyHomogeneousScalarArrays() throws IOException {
        String json = """
            {
              "tags": ["payment", "success"],
              "scores": [10, 20.5, 30],
              "flags": [true, false, true]
            }
            """;

        TypedLog log = parser.parse(json);

        assertAll(
                () -> assertEquals(
                        List.of("tags"),
                        log.stringArrayNames()
                ),
                () -> assertEquals(
                        List.of(List.of("payment", "success")),
                        log.stringArrayValues()
                ),
                () -> assertEquals(
                        List.of("scores"),
                        log.numberArrayNames()
                ),
                () -> assertEquals(
                        List.of(List.of(10.0, 20.5, 30.0)),
                        log.numberArrayValues()
                ),
                () -> assertEquals(
                        List.of("flags"),
                        log.booleanArrayNames()
                ),
                () -> assertEquals(
                        List.of(List.of(true, false, true)),
                        log.booleanArrayValues()
                ),
                () -> assertTrue(log.jsonNames().isEmpty())
        );
    }


    @Test
    void shouldPreserveComplexArraysWithoutReparsing()
            throws IOException {

        String json = """
            {
              "empty": [ ],
              "mixed": ["payment", 123],
              "nullable": ["payment", null],
              "items": [{"id":101}, {"id":102}],
              "matrix": [[1,2],[3,4]],
              "tags": ["payment", "success"],
              "scores": [10, 20.5],
              "flags": [true, false]
            }
            """;

        TypedLog log = parser.parse(json);

        assertAll(
                () -> assertEquals(
                        List.of("empty", "mixed", "nullable", "items", "matrix"),
                        log.jsonNames()
                ),
                () -> assertEquals(
                        List.of(
                                "[ ]",
                                "[\"payment\", 123]",
                                "[\"payment\", null]",
                                "[{\"id\":101}, {\"id\":102}]",
                                "[[1,2],[3,4]]"
                        ),
                        log.jsonValues()
                ),
                () -> assertEquals(
                        List.of(List.of("payment", "success")),
                        log.stringArrayValues()
                ),
                () -> assertEquals(
                        List.of(List.of(10.0, 20.5)),
                        log.numberArrayValues()
                ),
                () -> assertEquals(
                        List.of(List.of(true, false)),
                        log.booleanArrayValues()
                )
        );
    }


    @Test
    void shouldRejectMalformedJson() {
        String json = """
            {
              "event": "checkout",
              "user": {
                "id": 101
            """;

        assertThrows(
                IOException.class,
                () -> parser.parse(json)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[]",
            "123",
            "\"hello\"",
            "true",
            "null"
    })
    void shouldRejectNonObjectRoot(String json) {
        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(json)
        );
    }

    @Test
    void shouldRejectTrailingJsonContent() {
        String json = """
            {"id":1} {"id":2}
            """;

        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(json)
        );
    }

    @Test
    void shouldPreserveDuplicateFieldPaths() throws IOException {
        String json = """
            {
              "extra.age": 32,
              "extra": {
                "age": "32"
              },
              "age": 20,
              "age": 30
            }
            """;

        TypedLog log = parser.parse(json);

        assertAll(
                () -> assertEquals(
                        List.of("extra.age", "age", "age"),
                        log.numberNames()
                ),
                () -> assertEquals(
                        List.of(32.0, 20.0, 30.0),
                        log.numberValues()
                ),
                () -> assertEquals(
                        List.of("extra.age"),
                        log.stringNames()
                ),
                () -> assertEquals(
                        List.of("32"),
                        log.stringValues()
                )
        );
    }

    @Test
    void shouldIgnoreEmptyNestedObjects() throws IOException {
        String json = """
            {
              "event": "checkout",
              "metadata": {},
              "user": {
                "profile": {}
              }
            }
            """;

        TypedLog log = parser.parse(json);

        assertAll(
                () -> assertEquals(
                        List.of("event"),
                        log.stringNames()
                ),
                () -> assertEquals(
                        List.of("checkout"),
                        log.stringValues()
                ),
                () -> assertTrue(log.numberNames().isEmpty()),
                () -> assertTrue(log.booleanNames().isEmpty()),
                () -> assertTrue(log.nullNames().isEmpty()),
                () -> assertTrue(log.jsonNames().isEmpty())
        );
    }

    @Test
    void shouldDocumentDoublePrecisionLimit() throws IOException {
        String json = """
            {"id":9007199254740993}
            """;

        TypedLog log = parser.parse(json);

        // 2^53 + 1 không thể biểu diễn chính xác bằng IEEE-754 double.
        // Giá trị được làm tròn về 2^53.
        assertEquals(
                List.of(9007199254740992.0),
                log.numberValues()
        );

        assertEquals(
                json,
                log.rawSource()
        );
    }

    @Test
    void shouldDocumentDoublePrecisionLimitInArrays() throws IOException {
        String json = """
            {"ids":[9007199254740992,9007199254740993]}
            """;

        TypedLog log = parser.parse(json);

        assertEquals(
                List.of(List.of(
                        9007199254740992.0,
                        9007199254740992.0
                )),
                log.numberArrayValues()
        );
    }
}