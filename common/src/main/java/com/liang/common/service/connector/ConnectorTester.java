package com.liang.common.service.connector;

import cn.hutool.core.util.IdUtil;
import com.liang.common.dto.*;
import com.liang.common.service.connector.database.template.HbaseTemplate;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.service.connector.database.template.doris.DorisParquetWriter;
import com.liang.common.service.connector.database.template.doris.DorisWriter;
import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.service.connector.storage.parquet.TableParquetWriter;
import com.liang.common.service.connector.storage.parquet.schema.ReadableSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/*
  -- doris
  drop table test.demo;
  create table test.demo(id largeint,name string,age int,create_time datetime)unique key(id) distributed by hash(id) buckets 1;
  -- hive
  drop table flink.demo;
  create table flink.demo(id decimal(20,0),name string,age decimal(3,0),create_time string)stored as parquet location 'obs://hadoop-obs/flink/parquet/demo';
*/

@Slf4j
public class ConnectorTester {
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    // connector
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("427.test");
    private final ObsWriter obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
    private final HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
    private final DorisWriter dorisWriter = new DorisWriter("dorisSink", 128 * 1024 * 1024);
    private final DorisParquetWriter dorisParquetWriter = new DorisParquetWriter("dorisSink");
    private final TableParquetWriter tableParquetWriter = new TableParquetWriter("obs://hadoop-obs/flink/parquet/demo/", Arrays.asList(
            ReadableSchema.of("id", "bigint unsigned"),
            ReadableSchema.of("name", "varchar(255)"),
            ReadableSchema.of("age", "decimal(3,0)"),
            ReadableSchema.of("create_time", "datetime")
    ));

    {
        jdbcTemplate.enableCache();
        obsWriter.enableCache();
        hbaseTemplate.enableCache();
    }

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig();
        ConfigUtils.setConfig(config);
        ConnectorTester connectorTester = new ConnectorTester();
        connectorTester.invoke();
        connectorTester.flush();
    }

    public void invoke() {
        // hive jdbc
        try {
            Class.forName(DRIVER);
            try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery("show tables from test")) {
                        resultSet.next();
                    }
                }
            }
        } catch (Exception e) {
            String msg = "hive jdbc error";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
        // connector
        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", IdUtil.getSnowflakeNextIdStr());
        columnMap.put("name", UUID.randomUUID().toString());
        columnMap.put("age", 100);
        columnMap.put("create_time", now);
        jdbcTemplate.queryForColumnMaps("show tables from test");
        obsWriter.update(JsonUtils.toString(columnMap));
        hbaseTemplate.update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "22822").put("demo_key", System.currentTimeMillis() / 1000));
        dorisWriter.write(new DorisOneRow(DorisSchema.builder().database("test").tableName("demo").build()).putAll(columnMap));
        dorisParquetWriter.write(new DorisOneRow(DorisSchema.builder().database("test").tableName("demo").build()).putAll(columnMap));
        tableParquetWriter.write(columnMap);
    }

    public void flush() {
        jdbcTemplate.flush();
        obsWriter.flush();
        hbaseTemplate.flush();
        dorisWriter.flush();
        dorisParquetWriter.flush();
        tableParquetWriter.flush();
    }
}
