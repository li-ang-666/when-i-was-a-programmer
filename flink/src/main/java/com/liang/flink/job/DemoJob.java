package com.liang.flink.job;

import com.liang.common.dto.*;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.doris.DorisWriter;
import com.liang.common.service.storage.obs.ObsWriter;
import com.liang.common.service.storage.parquet.TableParquetWriter;
import com.liang.common.service.storage.parquet.schema.ReadableSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
@LocalConfigFile("demo.yml")
public class DemoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        testHiveJdbc();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new DemoSink(config))
                .name("DemoSink")
                .uid("DemoSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("DemoJob");
    }

    private static void testHiveJdbc() throws Exception {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        String user = "hive";
        String password = "";
        Class.forName(driver);
        DriverManager
                .getConnection(url, user, password)
                .createStatement()
                .executeQuery("show tables from test")
                .getMetaData();
    }

    @RequiredArgsConstructor
    private final static class DemoSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate jdbcTemplate;
        private ObsWriter obsWriter;
        private HbaseTemplate hbaseTemplate;
        private DorisWriter dorisWriter;
        private TableParquetWriter tableParquetWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("427.test");
            jdbcTemplate.enableCache();
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.TXT);
            obsWriter.enableCache();
            hbaseTemplate = new HbaseTemplate("hbaseSink");
            hbaseTemplate.enableCache();
            dorisWriter = new DorisWriter("dorisSink", 128 * 1024 * 1024);
            ArrayList<ReadableSchema> schemas = new ArrayList<>();
            schemas.add(ReadableSchema.of("id", "bigint unsigned"));
            schemas.add(ReadableSchema.of("name", "varchar(255)"));
            schemas.add(ReadableSchema.of("age", "decimal(3,0)"));
            tableParquetWriter = new TableParquetWriter("obs://hadoop-obs/flink/parquet/demo/", schemas);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            jdbcTemplate.queryForColumnMaps("show tables");
            obsWriter.update(JsonUtils.toString(new ArrayList<String>()));
            hbaseTemplate.update(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "22822").put("test_key", "0"));
            dorisWriter.write(new DorisOneRow(DorisSchema.builder().database("test").tableName("demo").build()).put("id", 1).put("name", "java"));
            HashMap<String, Object> columnMap = new HashMap<>();
            columnMap.put("id", 1);
            columnMap.put("name", "java");
            columnMap.put("age", 30);
            tableParquetWriter.write(columnMap);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            jdbcTemplate.flush();
            obsWriter.flush();
            hbaseTemplate.flush();
            tableParquetWriter.flush();
            dorisWriter.flush();
        }
    }
}
