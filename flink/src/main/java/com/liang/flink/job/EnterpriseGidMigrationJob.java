package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

@Slf4j
@LocalConfigFile("enterprise-gid-migration.yml")
public class EnterpriseGidMigrationJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("id"))
                .addSink(new EnterpriseGidMigrationSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EnterpriseGidMigrationSink")
                .uid("EnterpriseGidMigrationSink");
        env.execute("EnterpriseGidMigrationJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class EnterpriseGidMigrationSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final String SINK_TABLE = "enterprise_gid_migration";
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate("435.company_base");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String sql;
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String id = String.valueOf(columnMap.get("id"));
                sql = new SQL()
                        .DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
            } else {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                sql = new SQL()
                        .REPLACE_INTO(SINK_TABLE)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
            }
            sink.update(sql);
        }
    }
}
