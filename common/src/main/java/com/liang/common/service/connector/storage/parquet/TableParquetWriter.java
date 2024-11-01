package com.liang.common.service.connector.storage.parquet;

import com.liang.common.service.connector.storage.parquet.schema.ReadableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class TableParquetWriter {
    private static final int PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private final String path;
    private final Map<String, ReadableSchema> columnToSchema = new LinkedHashMap<>();
    private final Schema recordSchema;
    private volatile ParquetWriter<GenericRecord> writer = null;

    public TableParquetWriter(String path, List<ReadableSchema> columnSchemas) {
        this.path = (path.endsWith("/")) ? path : path + "/";
        SchemaBuilder.FieldAssembler<Schema> recordSchemaBuilder = SchemaBuilder.record("record").fields();
        columnSchemas.forEach(readableSchema -> {
            String columnName = readableSchema.getName();
            columnToSchema.put(columnName, readableSchema);
            recordSchemaBuilder
                    .name(columnName)
                    .type(readableSchema.getSchema())
                    .noDefault();
        });
        recordSchema = recordSchemaBuilder.endRecord();
    }

    public synchronized void write(Map<String, Object> columnMap) {
        try {
            if (writer == null) {
                writer = getWriter();
            }
            GenericData.Record record = getRecord(columnMap);
            writer.write(record);
        } catch (Exception e) {
            String msg = "FsParquetWriter write error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public synchronized void flush() {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        } catch (Exception e) {
            String msg = "FsParquetWriter flush error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private synchronized ParquetWriter<GenericRecord> getWriter() {
        try {
            return AvroParquetWriter.<GenericRecord>builder(new Path(path + UUID.randomUUID()))
                    .withSchema(recordSchema)
                    .withRowGroupSize(PARQUET_ROW_GROUP_SIZE)
                    .withDataModel(new GenericData() {{
                        addLogicalTypeConversion(new Conversions.DecimalConversion());
                    }})
                    .build();
        } catch (Exception e) {
            String msg = "FsParquetWriter getWriter error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private synchronized GenericData.Record getRecord(Map<String, Object> columnMap) {
        try {
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
            columnMap.forEach((column, value) -> {
                if (columnToSchema.containsKey(column)) {
                    recordBuilder.set(column, columnToSchema.get(column).formatValue(value));
                }
            });
            return recordBuilder.build();
        } catch (Exception e) {
            String msg = "FsParquetWriter getRecord error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
