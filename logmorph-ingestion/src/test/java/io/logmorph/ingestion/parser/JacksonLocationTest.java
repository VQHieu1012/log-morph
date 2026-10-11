
package io.logmorph.ingestion.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JacksonLocationTest {

    private final JsonFactory factory = new JsonFactory();

    @Test
    void shouldExtractExactJsonArrayFragments() throws IOException {
        String rawJson = """
                {
                  "empty": [ ],
                  "mixed": [ "hello", 123, true ],
                  "items": [
                    {"id": 101, "name": "Nam"},
                    {"id": 102, "name": "An"}
                  ],
                  "matrix": [[1, 2], [3, 4]],
                  "unicode": ["Tiếng Việt", "日本語"],
                  "escaped": ["a\\\\nb", "a]b", "{hello}"]
                }
                """;

        List<String> expected = List.of(
                "[ ]",
                "[ \"hello\", 123, true ]",
                """
                [
                    {"id": 101, "name": "Nam"},
                    {"id": 102, "name": "An"}
                  ]""",
                "[[1, 2], [3, 4]]",
                "[\"Tiếng Việt\", \"日本語\"]",
                "[\"a\\\\nb\", \"a]b\", \"{hello}\"]"
        );

        List<String> actual = new ArrayList<>();

        try (JsonParser parser = factory.createParser(rawJson)) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() != JsonToken.START_ARRAY) {
                    continue;
                }

                long start = parser.currentTokenLocation()
                        .getCharOffset();

                // Skip toàn bộ array, kể cả object/array lồng nhau.
                parser.skipChildren();

                assertEquals(
                        JsonToken.END_ARRAY,
                        parser.currentToken()
                );

                long end = parser.currentTokenLocation()
                        .getCharOffset();

                assertTrue(start >= 0, "Start offset unavailable");
                assertTrue(end >= start, "Invalid end offset");
                assertTrue(end < rawJson.length(), "Offset out of range");

                int from = Math.toIntExact(start);
                int to = Math.toIntExact(end) + 1;

                assertEquals('[', rawJson.charAt(from));
                assertEquals(']', rawJson.charAt(to - 1));

                String fragment = rawJson.substring(from, to);
                actual.add(fragment);

                System.out.printf(
                        "start=%d end=%d fragment=%s%n",
                        start,
                        end,
                        fragment
                );
            }
        }

        assertEquals(expected, actual);
    }

    @Test
    void shouldExtractExactJsonArrayFragmentsWithManualTraversal()
            throws IOException {

        String rawJson = """
                {
                  "empty": [ ],
                  "mixed": [ "hello", 123, true ],
                  "items": [
                    {"id": 101, "name": "Nam"},
                    {"id": 102, "name": "An"}
                  ],
                  "matrix": [[1, 2], [3, 4]],
                  "unicode": ["Tiếng Việt", "日本語"],
                  "escaped": ["a\\\\nb", "a]b", "{hello}"]
                }
                """;

        List<String> expected = List.of(
                "[ ]",
                "[ \"hello\", 123, true ]",
                """
                [
                    {"id": 101, "name": "Nam"},
                    {"id": 102, "name": "An"}
                  ]""",
                "[[1, 2], [3, 4]]",
                "[\"Tiếng Việt\", \"日本語\"]",
                "[\"a\\\\nb\", \"a]b\", \"{hello}\"]"
        );

        List<String> actual = new ArrayList<>();

        try (JsonParser parser = factory.createParser(rawJson)) {
            while (parser.nextToken() != null) {

                if (parser.currentToken() != JsonToken.START_ARRAY) {
                    continue;
                }

                // Parser đang đứng tại START_ARRAY.
                long start = parser.currentTokenLocation()
                        .getCharOffset();

                int depth = 1;

                // Duyệt thủ công toàn bộ array, kể cả nested structures.
                while (depth > 0) {
                    JsonToken token = parser.nextToken();

                    if (token == null) {
                        throw new IllegalArgumentException(
                                "Unexpected end of JSON array"
                        );
                    }

                    switch (token) {
                        case START_ARRAY, START_OBJECT -> depth++;
                        case END_ARRAY, END_OBJECT -> depth--;
                        default -> {
                            // Scalar tokens không thay đổi depth.
                        }
                    }
                }

                // Parser phải đang đứng tại END_ARRAY của array ban đầu.
                assertEquals(
                        JsonToken.END_ARRAY,
                        parser.currentToken()
                );

                long end = parser.currentTokenLocation()
                        .getCharOffset();

                assertTrue(start >= 0, "Start offset unavailable");
                assertTrue(end >= start, "Invalid end offset");
                assertTrue(end < rawJson.length(), "End offset out of range");

                int from = Math.toIntExact(start);
                int to = Math.toIntExact(end) + 1;

                assertEquals('[', rawJson.charAt(from));
                assertEquals(']', rawJson.charAt(to - 1));

                String fragment = rawJson.substring(from, to);

                actual.add(fragment);

                System.out.printf(
                        "start=%d, end=%d, fragment=%s%n",
                        start,
                        end,
                        fragment
                );
            }
        }

        assertEquals(expected, actual);
    }
}
