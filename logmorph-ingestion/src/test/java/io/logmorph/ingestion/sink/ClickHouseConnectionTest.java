package io.logmorph.ingestion.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import io.logmorph.model.TypedLog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;

import java.util.List;
import java.util.UUID;


class ClickHouseConnectionTest {

    @Test
    void shouldReadClickHouseTableSchema() {

        try (
                Client client = new Client.Builder()
                        .addEndpoint("http://localhost:8123")
                        .setUsername("logmorph")
                        .setPassword("logmorph123")
                        .compressServerResponse(false)
                        .serverSetting(
                                "http_x_clickhouse_format_overrides_output_format",
                                "0"
                        )
                        .build()
        ) {
            var schema = client.getTableSchema("logmorph.logs");

            assertNotNull(schema);
            System.out.println(schema);
        }
    }

    @Test
    void shouldRegisterTypedLogSerializer() {
        try (
                Client client = new Client.Builder()
                        .addEndpoint("http://localhost:8123")
                        .setUsername("logmorph")
                        .setPassword("logmorph123")
                        .compressServerResponse(false)
                        .serverSetting(
                                "http_x_clickhouse_format_overrides_output_format",
                                "0"
                        )
                        .build()
        ) {
            var schema = client.getTableSchema("logmorph.logs");

            assertDoesNotThrow(
                    () -> client.register(TypedLog.class, schema)
            );
        }
    }

    @Test
    void shouldInsertTypedLog() throws Exception {
        String marker = UUID.randomUUID().toString();

        try (
                Client client = new Client.Builder()
                        .addEndpoint("http://localhost:8123")
                        .setUsername("logmorph")
                        .setPassword("logmorph123")
                        .compressServerResponse(false)
                        .serverSetting(
                                "http_x_clickhouse_format_overrides_output_format",
                                "0"
                        )
                        .build()
        ) {
            var schema = client.getTableSchema("logmorph.logs");

            client.register(TypedLog.class, schema);

            TypedLog log = new TypedLog(
                    List.of("event"),
                    List.of("checkout"),

                    List.of("price"),
                    List.of(100.0),

                    List.of("success"),
                    List.of(true),

                    List.of("email"),

                    List.of("tags"),
                    List.of(List.of("payment", "success")),

                    List.of("scores"),
                    List.of(List.of(10.0, 20.0)),

                    List.of("flags"),
                    List.of(List.of(true, false)),

                    List.of("items"),
                    List.of("[{\"product_id\":101}]"),

                    "{\"test_marker\":\"" + marker + "\"}"
            );

            try (var response = client.insert(
                    "logmorph.logs",
                    List.of(log)
            ).get()) {
                assertEquals(1L, response.getWrittenRows());
            }

            System.out.println("Inserted marker: " + marker);
        }
    }

    @Test
    void shouldInsertTypedLogWithRowBinary() throws Exception {
        String marker = UUID.randomUUID().toString();

        TypedLog log = new TypedLog(
                List.of("event", "test_marker"),
                List.of("checkout", marker),

                List.of("price"),
                List.of(100.0),

                List.of("success"),
                List.of(true),

                List.of("email"),

                List.of("tags"),
                List.of(List.of("payment", "success")),

                List.of("scores"),
                List.of(List.of(10.0, 20.0)),

                List.of("flags"),
                List.of(List.of(true, false)),

                List.of("items"),
                List.of("[{\"product_id\":101}]"),

                "{\"test_marker\":\"" + marker + "\"}"
        );

        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("logmorph")
                .setPassword("logmorph123")
                .compressServerResponse(false)
                .serverSetting(
                        "http_x_clickhouse_format_overrides_output_format",
                        "0"
                )
                .build()) {

            TableSchema fullSchema =
                    client.getTableSchema("logmorph.logs");

            List<String> columns = ClickHouseInsertColumns.NAMES;

            // Lấy metadata theo đúng thứ tự của insert contract.
            List<ClickHouseColumn> columnDefinitions = columns.stream()
                    .map(name -> {
                        ClickHouseColumn column =
                                fullSchema.getColumnByName(name);

                        if (column == null) {
                            throw new IllegalStateException(
                                    "Missing ClickHouse column: " + name
                            );
                        }

                        return column;
                    })
                    .toList();

            // Writer chỉ biết 16 cột của ingestion.
            TableSchema writerSchema = new TableSchema(columnDefinitions);

            try (var response = client.insert(
                    "logmorph.logs",
                    columns,
                    output -> {
                        RowBinaryFormatWriter writer =
                                new RowBinaryFormatWriter(
                                        output,
                                        writerSchema,
                                        ClickHouseFormat.RowBinary
                                );

                        writer.setList("string_names", log.stringNames());
                        writer.setList("string_values", log.stringValues());

                        writer.setList("number_names", log.numberNames());
                        writer.setList("number_values", log.numberValues());

                        writer.setList("boolean_names", log.booleanNames());
                        writer.setList("boolean_values", log.booleanValues());

                        writer.setList("null_names", log.nullNames());

                        writer.setList(
                                "string_array_names",
                                log.stringArrayNames()
                        );
                        writer.setList(
                                "string_array_values",
                                log.stringArrayValues()
                        );

                        writer.setList(
                                "number_array_names",
                                log.numberArrayNames()
                        );
                        writer.setList(
                                "number_array_values",
                                log.numberArrayValues()
                        );

                        writer.setList(
                                "boolean_array_names",
                                log.booleanArrayNames()
                        );
                        writer.setList(
                                "boolean_array_values",
                                log.booleanArrayValues()
                        );

                        writer.setList("json_names", log.jsonNames());
                        writer.setList("json_values", log.jsonValues());

                        writer.setString("raw_source", log.rawSource());

                        writer.commitRow();
                    },
                    ClickHouseFormat.RowBinary,
                    new InsertSettings()
            ).get()) {
                assertEquals(1L, response.getWrittenRows());
            }

            System.out.println("Inserted marker: " + marker);
        }
    }

}