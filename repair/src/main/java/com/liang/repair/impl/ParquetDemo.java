package com.liang.repair.impl;

import com.liang.repair.service.ConfigHolder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.UUID;

public class ParquetDemo extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("DorisOneRow").fields()
                .name("id").type().stringType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("__DORIS_DELETE_SIGN__").type().stringType().noDefault()
                .endRecord();
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path("file:///Users/liang/Desktop/parquet-demo.parquet.gz"))
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withSchema(schema)
                .build();
        for (int i = 1; i <= 21000000; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", i);
            record.put("name", UUID.randomUUID().toString());
            record.put("__DORIS_DELETE_SIGN__", 0);
            writer.write(record);
        }
        writer.close();
        log.info("finish");
    }
}