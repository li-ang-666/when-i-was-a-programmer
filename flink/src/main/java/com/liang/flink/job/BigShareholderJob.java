package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@LocalConfigFile("big-shareholder.yml")
public class BigShareholderJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new BigShareholderSink(config))
                .name("BigShareholderSink")
                .uid("BigShareholderSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("BigShareholderJob");
    }

    @RequiredArgsConstructor
    private final static class BigShareholderSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private static final Set<String> Listed_company;

        static {
            InputStream stream = BigShareholderJob.class.getClassLoader()
                    .getResourceAsStream("equity_ratio_source_100_company.txt");
            String[] arr = IoUtil.read(stream, StandardCharsets.UTF_8)
                    .trim()
                    .split("\n");
            Listed_company = Arrays.stream(arr)
                    .filter(e -> e.matches(".*?(\\d+).*"))
                    .map(e -> e.replaceAll(".*?(\\d+).*", "$1"))
                    .collect(Collectors.toSet());
        }

        private final Config config;
        private JdbcTemplate jdbcTemplate457;
        private JdbcTemplate jdbcTemplate463;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate457 = new JdbcTemplate("457.prism_shareholder_path");
            jdbcTemplate457.enableCache();
            jdbcTemplate463 = new JdbcTemplate("463.bdp_equity");
            jdbcTemplate463.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if (table.equals("ratio_path_company")) {
                String companyId = String.valueOf(columnMap.get("company_id"));
                String isBigShareholder = String.valueOf(columnMap.get("is_big_shareholder"));
                if ("1".equals(isBigShareholder) && !Listed_company.contains(companyId)) {
                    String update = new SQL().UPDATE("ratio_path_company")
                            .SET("is_big_shareholder = 0")
                            .WHERE("id = " + columnMap.get("id"))
                            .toString();
                    jdbcTemplate457.update(update);
                }
            } else if (table.equals("shareholder_identity_type_details")) {
                String companyId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
                String shareholderIdentityType = String.valueOf(columnMap.get("shareholder_identity_type"));
                if ("1".equals(shareholderIdentityType) && !Listed_company.contains(companyId)) {
                    String delete = new SQL().DELETE_FROM("shareholder_identity_type_details")
                            .WHERE("id = " + columnMap.get("id"))
                            .toString();
                    jdbcTemplate463.update(delete);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }

        @Override
        public void finish() {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }

        @Override
        public void close() {
            jdbcTemplate457.flush();
            jdbcTemplate463.flush();
        }
    }
}
