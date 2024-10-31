package com.liang.flink.job;


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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@LocalConfigFile("bid-incr.yml")
public class BidIncrJob {
    private static final List<String> SINK_RDS = Arrays.asList(
            "volcanic_cloud_0",
            "volcanic_cloud_1",
            "volcanic_cloud_2",
            "volcanic_cloud_3",
            "volcanic_cloud_4",
            "volcanic_cloud_5",
            "volcanic_cloud_6",
            "volcanic_cloud_7",
            "volcanic_cloud_8",
            "volcanic_cloud_9",
            "volcanic_cloud_10"
    );
    private static final String SINK_TABlE = "company_bid_incr";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .flatMap(new BidIncrMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidIncrMapper")
                .uid("BidIncrMapper")
                .keyBy(e -> (String) e.get("id"))
                .addSink(new BidIncrSink(config))
                .setParallelism(11)
                .name("BidIncrSink")
                .uid("BidIncrSink");
        env.execute("BidIncrJob");
    }

    @RequiredArgsConstructor
    private static final class BidIncrMapper extends RichFlatMapFunction<SingleCanalBinlog, Map<String, Object>> {
        private final Config config;
        private JdbcTemplate query;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String rds = SINK_RDS.get(taskIdx % SINK_RDS.size());
            log.info("mapper_{} -> {}", taskIdx, rds);
            query = new JdbcTemplate(rds);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Map<String, Object>> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            Object id = columnMap.get("id");
            // 判断是否已经处理过
            String sql1 = new SQL().SELECT("1").FROM("company_bid").WHERE("id = " + id).toString();
            String sql2 = new SQL().SELECT("1").FROM("company_bid_fail").WHERE("id = " + id).toString();
            String sql3 = new SQL().SELECT("1").FROM("company_bid_empty").WHERE("id = " + id).toString();
            String sql4 = new SQL().SELECT("1").FROM("company_bid_incr").WHERE("id = " + id).toString();
            String sql = sql1 + " union all " + sql2 + " union all " + sql3 + " union all " + sql4;
            String queryRes = query.queryForObject(sql, rs -> rs.getString(1));
            if (queryRes == null) {
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("id", id);
                out.collect(resultMap);
            }
        }
    }

    @RequiredArgsConstructor
    private static final class BidIncrSink extends RichSinkFunction<Map<String, Object>> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate source;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            source = new JdbcTemplate("104.data_bid");
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String rds = SINK_RDS.get(taskIdx);
            log.info("sink_{} -> {}", taskIdx, rds);
            sink = new JdbcTemplate(rds);
            sink.enableCache();
        }

        @Override
        public void invoke(Map<String, Object> resultMap, Context context) {
            String querySql = new SQL().SELECT("id,uuid,title,content,type,deleted")
                    .FROM("company_bid")
                    .WHERE("id = " + resultMap.get("id"))
                    .toString();
            List<Map<String, Object>> columnMaps = source.queryForColumnMaps(querySql);
            if (columnMaps.isEmpty()) {
                return;
            }
            Map<String, Object> columnMap = columnMaps.get(0);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String writeSql = new SQL().REPLACE_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(writeSql);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
