package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.connector.database.template.doris.DorisParquetWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class DorisJob {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // sql
        String sparkSql = parameterTool.get("sparkSql");
        log.info("sparkSql: {}", sparkSql);
        // db
        String sinkDatabase = parameterTool.get("sinkDatabase");
        log.info("sinkDatabase: {}", sinkDatabase);
        // tb
        String sinkTable = parameterTool.get("sinkTable");
        log.info("sinkTable: {}", sinkTable);
        // exec
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.sql(sparkSql)
                .repartition()
                .foreachPartition(new DorisSink(ConfigUtils.getConfig(), sinkDatabase, sinkTable));
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProperties())) {
            HashMap<String, Object> kafkaColumnMap = new LinkedHashMap<>();
            kafkaColumnMap.put("dbType", "DORIS");
            kafkaColumnMap.put("taskId", 19999);
            kafkaColumnMap.put("database", sinkDatabase);
            kafkaColumnMap.put("table", sinkTable);
            kafkaColumnMap.put("pt", LocalDateTime.now().plusDays(-1).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            kafkaColumnMap.put("syncStatus", "success");
            kafkaColumnMap.put("timestamp", System.currentTimeMillis());
            kafkaColumnMap.put("operator", "liang");
            kafkaProducer.send(new ProducerRecord<>("user_tag_status", JsonUtils.toString(kafkaColumnMap)));
            kafkaProducer.flush();
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return properties;
    }

    @Slf4j
    @RequiredArgsConstructor
    public final static class DorisSink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final String database;
        private final String table;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            DorisParquetWriter dorisWriter = new DorisParquetWriter("dorisSink");
            DorisSchema schema = DorisSchema.builder().database(database).tableName(table).build();
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                dorisWriter.write(new DorisOneRow(schema, columnMap));
            }
            dorisWriter.flush();
        }
    }
}
