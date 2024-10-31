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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

@Slf4j
@LocalConfigFile("graph-annual.yml")
public class GraphAnnualJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new GraphAnnualSink(config))
                .name("GraphAnnualSink")
                .uid("GraphAnnualSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("GraphAnnualJob");
    }

    @RequiredArgsConstructor
    private final static class GraphAnnualSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate graphData;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            graphData = new JdbcTemplate("430.graph_data");
            graphData.enableCache(1000, 128);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.remove("id");
            columnMap.put("reference_pt_year", 2024);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
            String sql = new SQL().INSERT_IGNORE_INTO("company_equity_relation_details_tmp")
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            graphData.update(sql);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            graphData.flush();
        }

        @Override
        public void finish() {
            graphData.flush();
        }

        @Override
        public void close() {
            graphData.flush();
        }
    }
}
