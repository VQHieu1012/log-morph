package io.logmorph.ingestion.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import io.logmorph.model.TypedLog;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public final class ClickHouseBatchWriter {
    private final Client client;
    private final String tableName;
    private final TableSchema writerSchema;

    public ClickHouseBatchWriter(Client client, String tableName) {
        this.client = Objects.requireNonNull(client, "client");
        this.tableName = Objects.requireNonNull(tableName, "tableName");

        TableSchema fullSchema = client.getTableSchema(tableName);

        List<ClickHouseColumn> columns = ClickHouseInsertColumns.NAMES.stream()
                .map(name -> {
                    ClickHouseColumn column = fullSchema.getColumnByName(name);
                    if (column == null) {
                        throw new IllegalStateException(
                                "Missing Clickhouse column: " + name
                        );
                    }

                    return column;
                }).toList();

        this.writerSchema = new TableSchema(columns);
    }

    public void write(List<TypedLog> batch) throws InterruptedException, ExecutionException {
        Objects.requireNonNull(batch, "batch");

        if (batch.isEmpty()) return;

        try (var response = client.insert(
                tableName,
                ClickHouseInsertColumns.NAMES,
                output -> {
                    RowBinaryFormatWriter writer = new RowBinaryFormatWriter(
                            output, writerSchema, ClickHouseFormat.RowBinary
                    );
                    for (TypedLog log : batch) {
                        writeRow(writer, log);
                    }
                },
                ClickHouseFormat.RowBinary,
                new InsertSettings()
        ).get()) {
            // Insert done, close response
        }
    }

    private static void writeRow(RowBinaryFormatWriter writer,TypedLog log) throws IOException {
        writer.setList("string_names", log.stringNames());
        writer.setList("string_values", log.stringValues());

        writer.setList("number_names", log.numberNames());
        writer.setList("number_values", log.numberValues());

        writer.setList("boolean_names", log.booleanNames());
        writer.setList("boolean_values", log.booleanValues());

        writer.setList("null_names", log.nullNames());

        writer.setList("string_array_names", log.stringArrayNames());
        writer.setList("string_array_values", log.stringArrayValues());

        writer.setList("number_array_names", log.numberArrayNames());
        writer.setList("number_array_values", log.numberArrayValues());

        writer.setList("boolean_array_names", log.booleanArrayNames());
        writer.setList("boolean_array_values", log.booleanArrayValues());

        writer.setList("json_names", log.jsonNames());
        writer.setList("json_values", log.jsonValues());

        writer.setString("raw_source", log.rawSource());

        writer.commitRow();
    }
}
