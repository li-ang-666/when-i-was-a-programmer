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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConnectorTester {
    private JdbcTemplate jdbcTemplate;
    private ObsWriter obsWriter;
    private HbaseTemplate hbaseTemplate;
    private DorisWriter dorisWriter;
    private DorisParquetWriter dorisParquetWriter;
    private TableParquetWriter tableParquetWriter;

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig();
        ConfigUtils.setConfig(config);
        ConnectorTester connectorTester = new ConnectorTester();
        connectorTester.open();
        connectorTester.invoke();
        connectorTester.flush();
    }

    public void open() {
        jdbcTemplate = new JdbcTemplate("427.test");
        jdbcTemplate.enableCache();
        obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
        obsWriter.enableCache();
        hbaseTemplate = new HbaseTemplate("hbaseSink");
        hbaseTemplate.enableCache();
        dorisWriter = new DorisWriter("dorisSink", 128 * 1024 * 1024);
        dorisParquetWriter = new DorisParquetWriter("dorisSink");
        tableParquetWriter = new TableParquetWriter("obs://hadoop-obs/flink/parquet/demo/", Arrays.asList(
                ReadableSchema.of("id", "bigint unsigned"),
                ReadableSchema.of("name", "varchar(255)"),
                ReadableSchema.of("age", "decimal(3,0)")
        ));
    }

    public void invoke() {
        Map<String, Object> columnMap = new HashMap<String, Object>() {{
            put("id", IdUtil.getSnowflakeNextIdStr());
            put("name", UUID.randomUUID().toString());
            put("age", 100);
        }};
        jdbcTemplate.queryForColumnMaps("show tables");
        obsWriter.update(JsonUtils.toString(columnMap));
        hbaseTemplate.update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "22822").put("demo_key", "0"));
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
