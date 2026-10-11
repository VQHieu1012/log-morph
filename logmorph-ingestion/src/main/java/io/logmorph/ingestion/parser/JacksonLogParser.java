package io.logmorph.ingestion.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.logmorph.model.TypedLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public final class JacksonLogParser {

    private final JsonFactory jsonFactory;

    public JacksonLogParser() {
        this.jsonFactory = new JsonFactory();
    }

    private void parseObject(
            JsonParser parser,
            String prefix,
            String rawJson,
            Fields fields
    ) throws IOException {
        while (true) {
            JsonToken token = parser.nextToken();

            if (token == null) {
                throw new IllegalArgumentException(
                        "Unexpected end of JSON object"
                );
            }

            if (token == JsonToken.END_OBJECT) {
                break;
            }

            if (token != JsonToken.FIELD_NAME) {
                throw new IllegalArgumentException(
                        "Expected JSON field name"
                );
            }

            String fieldName = parser.currentName();

            String path = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            JsonToken valueToken = parser.nextToken();

            if (valueToken == null) {
                throw new IllegalArgumentException(
                        "Unexpected end of JSON"
                );
            }

            switch (valueToken) {
                case START_OBJECT -> parseObject(parser, path, rawJson, fields);
                case VALUE_STRING -> {
                    fields.stringNames.add(path);
                    fields.stringValues.add(parser.getText());
                }
                case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                    fields.numberNames.add(path);
                    fields.numberValues.add(parser.getDoubleValue());
                }
                case VALUE_TRUE, VALUE_FALSE -> {
                    fields.booleanNames.add(path);
                    fields.booleanValues.add(valueToken == JsonToken.VALUE_TRUE);
                }
                case VALUE_NULL -> fields.nullNames.add(path);
                case START_ARRAY -> parseArray(parser, path, rawJson, fields);
                default -> throw new IllegalArgumentException(
                        "Unexpected JSON token: " + valueToken
                );
            }

        }
    }

    private void parseArray(
            JsonParser parser,
            String path,
            String rawJson,
            Fields fields
    ) throws IOException {
        int start = Math.toIntExact(
                parser.currentTokenLocation().getCharOffset()
        );

        ArrayKind kind = null;

        List<String> strings = null;
        List<Double> numbers = null;
        List<Boolean> booleans = null;

        while (true) {
            JsonToken token = parser.nextToken();

            if (token == null) {
                throw new IllegalArgumentException(
                        "Unexpected end of JSON array"
                );
            }

            if (token == JsonToken.END_ARRAY) {
                break;
            }

            if (kind == ArrayKind.COMPLEX) {
                if (token == JsonToken.START_ARRAY
                        || token == JsonToken.START_OBJECT) {
                    parser.skipChildren();
                }
                continue;
            }

            ArrayKind elementKind = switch (token) {
                case VALUE_STRING -> ArrayKind.STRING;
                case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> ArrayKind.NUMBER;
                case VALUE_TRUE, VALUE_FALSE -> ArrayKind.BOOLEAN;
                default -> ArrayKind.COMPLEX;
            };

            if (elementKind == ArrayKind.COMPLEX || (kind != null && kind != elementKind)) {

                kind = ArrayKind.COMPLEX;

                if (token == JsonToken.START_ARRAY
                        || token == JsonToken.START_OBJECT) {
                    parser.skipChildren();
                }

                strings = null;
                numbers = null;
                booleans = null;
                continue;
            }

            if (kind == null) {
                kind = elementKind;
            }

            switch (kind) {
                case STRING -> {
                    if (strings == null) {
                        strings = new ArrayList<>();
                    }
                    strings.add(parser.getText());
                }

                case NUMBER -> {
                    if (numbers == null) {
                        numbers = new ArrayList<>();
                    }
                    numbers.add(parser.getDoubleValue());
                }

                case BOOLEAN -> {
                    if (booleans == null) {
                        booleans = new ArrayList<>();
                    }
                    booleans.add(token == JsonToken.VALUE_TRUE);
                }

                case COMPLEX -> {
                    // Đã xử lý phía trên.
                }
            }
        }

        // Postcondition: parser đang ở END_ARRAY.
        if (kind == ArrayKind.STRING) {
            fields.stringArrayNames.add(path);
            fields.stringArrayValues.add(strings);
            return;
        }

        if (kind == ArrayKind.NUMBER) {
            fields.numberArrayNames.add(path);
            fields.numberArrayValues.add(numbers);
            return;
        }

        if (kind == ArrayKind.BOOLEAN) {
            fields.booleanArrayNames.add(path);
            fields.booleanArrayValues.add(booleans);
            return;
        }

        // COMPLEX hoặc array rỗng.
        int end = Math.toIntExact(
                parser.currentTokenLocation().getCharOffset()
        );

        // Bảo vệ giả định về character offsets.
        if (start < 0 || end < start
                || end >= rawJson.length()
                || rawJson.charAt(start) != '['
                || rawJson.charAt(end) != ']') {
            throw new IllegalStateException(
                    "Invalid JSON array character offsets"
            );
        }

        fields.jsonNames.add(path);
        fields.jsonValues.add(
                rawJson.substring(start, end + 1)
        );
    }

    public TypedLog parse(String rawJson) throws IOException {
        Objects.requireNonNull(rawJson, "rawJson");

        try (JsonParser parser = jsonFactory.createParser(rawJson)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalArgumentException(
                        "Root JSON value must be an object"
                );
            }

            Fields fields = new Fields();

            parseObject(parser, "", rawJson, fields);

            if (parser.nextToken() != null) {
                throw new IllegalArgumentException(
                        "Unexpected content after root JSON object"
                );
            }

            return new TypedLog(
                    fields.stringNames,
                    fields.stringValues,

                    fields.numberNames,
                    fields.numberValues,

                    fields.booleanNames,
                    fields.booleanValues,

                    fields.nullNames,

                    fields.stringArrayNames,
                    fields.stringArrayValues,

                    fields.numberArrayNames,
                    fields.numberArrayValues,

                    fields.booleanArrayNames,
                    fields.booleanArrayValues,

                    fields.jsonNames,
                    fields.jsonValues,

                    rawJson
            );
        }
    }

    private enum ArrayKind {
        STRING,
        NUMBER,
        BOOLEAN,
        COMPLEX
    }

    private static final class Fields {
        final List<String> stringNames = new ArrayList<>();
        final List<String> stringValues = new ArrayList<>();

        final List<String> numberNames = new ArrayList<>();
        final List<Double> numberValues = new ArrayList<>();

        final List<String> booleanNames = new ArrayList<>();
        final List<Boolean> booleanValues = new ArrayList<>();

        final List<String> nullNames = new ArrayList<>();

        final List<String> stringArrayNames = new ArrayList<>();
        final List<List<String>> stringArrayValues = new ArrayList<>();

        final List<String> numberArrayNames = new ArrayList<>();
        final List<List<Double>> numberArrayValues = new ArrayList<>();

        final List<String> booleanArrayNames = new ArrayList<>();
        final List<List<Boolean>> booleanArrayValues = new ArrayList<>();

        final List<String> jsonNames = new ArrayList<>();
        final List<String> jsonValues = new ArrayList<>();
    }
}

