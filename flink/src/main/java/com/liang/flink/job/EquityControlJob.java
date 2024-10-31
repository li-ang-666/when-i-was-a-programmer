package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.equity.controller.EquityControlService;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@LocalConfigFile("equity-control.yml")
public class EquityControlJob {
    private static final String SINK_SOURCE = "463.bdp_equity";
    private static final String SINK_TABLE = "entity_controller_details_new";

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
                .map(new EquityControlMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlMapper")
                .uid("EquityControlMapper")
                .keyBy(e -> e)
                .addSink(new EquityControlSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityControlSink")
                .uid("EquityControlSink");
        env.execute("EquityControlJob");
    }

    @RequiredArgsConstructor
    private static final class EquityControlMapper extends RichMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public String map(SingleCanalBinlog singleCanalBinlog) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            // 公司维表
            if (table.contains("company_index")) {
                return String.valueOf(columnMap.get("company_id"));
            }
            // 穿透表
            else if (table.contains("ratio_path_company_new")) {
                return String.valueOf(columnMap.get("company_id"));
            }
            // 上市公告实控人
            else if (table.contains("stock_actual_controller")) {
                return String.valueOf(columnMap.get("graph_id"));
            }
            // 返回随机负数
            return "-" + new Random().nextInt(1024);
        }
    }

    @RequiredArgsConstructor
    private static final class EquityControlSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private final Config config;
        private EquityControlService service;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityControlService();
            sink = new JdbcTemplate(SINK_SOURCE);
            sink.enableCache();
        }

        @Override
        public void invoke(String companyId, Context context) {
            synchronized (bitmap) {
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                }
                // 全量修复的时候, 来一条计算一条
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.REPAIR) {
                    flush();
                }
            }
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
            synchronized (bitmap) {
                bitmap.forEach(this::consume);
                sink.flush();
                bitmap.clear();
            }
        }

        private void consume(Long companyId) {
            List<Map<String, Object>> columnMaps = service.processControl(String.valueOf(companyId));
            String deleteSql = new SQL()
                    .DELETE_FROM(SINK_TABLE)
                    .WHERE("company_id_controlled = " + SqlUtils.formatValue(companyId))
                    .toString();
            sink.update(deleteSql);
            for (Map<String, Object> columnMap : columnMaps) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String insertSql = new SQL()
                        .INSERT_INTO(SINK_TABLE)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
            }
        }
    }
}
