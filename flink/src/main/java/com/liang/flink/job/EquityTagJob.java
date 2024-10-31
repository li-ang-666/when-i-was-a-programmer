package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.*;
import java.util.concurrent.TimeUnit;

@LocalConfigFile("equity-tag.yml")
public class EquityTagJob {
    private static final Set<String> VALID_COLUMNS = new HashSet<>(Arrays.asList(
            // 公司
            "company_id",
            "company_name",
            "company_is_listed",
            "company_uscc",
            "company_org_type",
            "company_entity_property",
            // 股东
            "shareholder_entity_type",
            "shareholder_id",
            "shareholder_name",
            "shareholder_name_id",
            "shareholder_master_company_id",
            // 投资
            "is_direct_shareholder",
            "investment_ratio_direct",
            "investment_ratio_total",
            "is_big_shareholder",
            "is_controlling_shareholder"
    ));
    private static final String QUERY_RDS = "491.prism_shareholder_path";
    private static final String QUERY_TABLE = "ratio_path_company_new";

    private static final String SINK_RDS = "463.bdp_equity";
    private static final String SINK_TABLE = "shareholder_identity_type_details_new";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        if (!(env instanceof LocalStreamEnvironment)) {
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            // 运行周期
            checkpointConfig.setCheckpointInterval(TimeUnit.MINUTES.toMillis(10));
            // 两次checkpoint之间最少间隔时间
            checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(10));
        }
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .flatMap(new EquityTagFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityTagFlatMapper")
                .uid("EquityTagFlatMapper")
                .keyBy(e -> e)
                .addSink(new EquityTagSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityTagSink")
                .uid("EquityTagSink");
        env.execute("EquityTagJob");
    }

    @RequiredArgsConstructor
    private static final class EquityTagFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            out.collect(String.valueOf(columnMap.get("company_id")));
        }
    }

    @RequiredArgsConstructor
    private static final class EquityTagSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private final Config config;
        private JdbcTemplate source;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            source = new JdbcTemplate(QUERY_RDS);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
        }

        @Override
        public void invoke(String companyId, Context context) {
            synchronized (bitmap) {
                bitmap.add(Long.parseLong(companyId));
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.REPAIR) {
                    flush();
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
            sink.flush();
        }

        @Override
        public void finish() {
            flush();
            sink.flush();
        }

        @Override
        public void close() {
            flush();
            sink.flush();
        }

        private void flush() {
            synchronized (bitmap) {
                bitmap.forEach(companyId -> {
                    String deleteSql = new SQL().DELETE_FROM(SINK_TABLE)
                            .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                            .toString();
                    sink.update(deleteSql);
                    List<Map<String, Object>> columnMaps = queryRatioPathCompany(companyId);
                    for (Map<String, Object> columnMap : columnMaps) {
                        columnMap.entrySet().removeIf(entry -> !VALID_COLUMNS.contains(entry.getKey()));
                        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                        String insertSql = new SQL().INSERT_INTO(SINK_TABLE)
                                .INTO_COLUMNS(insert.f0)
                                .INTO_VALUES(insert.f1)
                                .toString();
                        sink.update(insertSql);
                    }
                });
                bitmap.clear();
            }
        }

        private List<Map<String, Object>> queryRatioPathCompany(Long companyId) {
            String sql = new SQL().SELECT("*")
                    .FROM(QUERY_TABLE + "_" + companyId % 100)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("(is_big_shareholder = 1 OR is_controlling_shareholder = 1)")
                    .WHERE("company_name <> shareholder_name")
                    .WHERE("company_id <> shareholder_id")
                    .toString();
            return source.queryForColumnMaps(sql);
        }
    }
}
