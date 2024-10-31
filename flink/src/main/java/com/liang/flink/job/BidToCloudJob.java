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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@LocalConfigFile("bid-to-cloud.yml")
public class BidToCloudJob {
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
    private static final String SINK_TABlE = "company_bid";
    private static final String SINK_TABlE_FAIL = "company_bid_empty";
//    private static final String SINK_TABlE_FAIL = "company_bid_fail";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> e.getColumnMap().get("uuid"))
                .flatMap(new BidToCloudMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidToCloudMapper")
                .uid("BidToCloudMapper")
                .keyBy(e -> (String) e.get("id"))
                .addSink(new BidToCloudSink(config))
                .setParallelism(11)
                .name("BidToCloudSink")
                .uid("BidToCloudSink");
        env.execute("BidToCloudJob");
    }

    @RequiredArgsConstructor
    private static final class BidToCloudMapper extends RichFlatMapFunction<SingleCanalBinlog, Map<String, Object>> {
        private final Config config;
        private ExecutorService executor;
        private JdbcTemplate query;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String rds = SINK_RDS.get(taskIdx % SINK_RDS.size());
            log.info("mapper_{} -> {}", taskIdx, rds);
            query = new JdbcTemplate(rds);
            executor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Map<String, Object>> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            // 判断是否已经处理过
            String sql1 = new SQL().SELECT("content").FROM(SINK_TABlE).WHERE("id = " + columnMap.get("id")).toString();
            String queryRes = query.queryForObject(sql1, rs -> rs.getString(1));
            if (queryRes != null && !queryRes.replaceAll("\\s", "").isEmpty()) {
                return;
            }
            HashMap<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", columnMap.get("id"));
            resultMap.put("uuid", columnMap.get("uuid"));
            resultMap.put("title", columnMap.get("title"));
            resultMap.put("deleted", columnMap.get("deleted"));
            resultMap.put("type", columnMap.get("type"));
            // html 转 md
            resultMap.put("content", columnMap.get("content"));
            resultMap.put("fail", true);
            out.collect(resultMap);
        }

//        private String htmlToMd(String html) {
//            MutableDataSet options = new MutableDataSet();
//            FlexmarkHtmlConverter converter = FlexmarkHtmlConverter.builder(options).build();
//            return converter.convert(html);
//        }
    }

    @RequiredArgsConstructor
    private static final class BidToCloudSink extends RichSinkFunction<Map<String, Object>> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String rds = SINK_RDS.get(taskIdx);
            log.info("sink_{} -> {}", taskIdx, rds);
            sink = new JdbcTemplate(rds);
            sink.enableCache();
        }

        @Override
        public void invoke(Map<String, Object> resultMap, Context context) {
            String table = ((boolean) resultMap.remove("fail")) ? SINK_TABlE_FAIL : SINK_TABlE;
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().REPLACE_INTO(table)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(sql);
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
