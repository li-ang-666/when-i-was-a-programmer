package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
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
import com.liang.flink.project.equity.bfs.EquityBfsService;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.stream.Collectors;

@LocalConfigFile("equity-bfs.yml")
public class EquityBfsJob {
    private static final String SINK_SOURCE = "491.prism_shareholder_path";
    private static final String SINK_TABLE = "ratio_path_company_new";

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
                // 根据股东id, 查询所有可能需要重新穿透的公司
                .rebalance()
                .flatMap(new EquityBfsFlatMapper(config))
                .setParallelism(32)
                .name("EquityBfsFlatMapper")
                .uid("EquityBfsFlatMapper")
                // 股权穿透
                .keyBy(companyId -> companyId)
                .flatMap(new EquityBfsCalculator(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityBfsCalculator")
                .uid("EquityBfsCalculator")
                // 写入mysql
                .keyBy(companyIdAndColumnMaps -> companyIdAndColumnMaps.f0)
                .addSink(new EquityBfsSink(config))
                .setParallelism(32)
                .name("EquityBfsSink")
                .uid("EquityBfsSink");
        env.execute("EquityBfsJob");
    }

    /**
     * 查询所有可能需要重新穿透的公司
     */
    @RequiredArgsConstructor
    private static final class EquityBfsFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_SOURCE);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            // ee59d.proto.company_base.company_index
            // 1ae09.proto.graph_data.company_equity_relation_details
            String table = singleCanalBinlog.getTable();
            Set<String> entityIds = new LinkedHashSet<>();
            if (StrUtil.equalsAny(table, "company_index", "company_equity_relation_details") && singleCanalBinlog.getEventType() != CanalEntry.EventType.UPDATE) {
                entityIds.add(StrUtil.blankToDefault((String) singleCanalBinlog.getColumnMap().get("company_id"), ""));
            }
            for (String entityId : entityIds) {
                if (!TycUtils.isTycUniqueEntityId(entityId)) {
                    continue;
                }
                if (TycUtils.isUnsignedId(entityId)) {
                    out.collect(entityId);
                }
                // 全量repair的时候不走这里
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.REPAIR) {
                    continue;
                }
                List<String> sqls = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    String sql = String.format("select distinct company_id from %s_%s where shareholder_id = %s", SINK_TABLE, i, SqlUtils.formatValue(entityId));
                    sqls.add(sql);
                }
                String sql = sqls.stream().collect(Collectors.joining(" union all ", "select distinct company_id from (", ") t"));
                sink.queryForList(sql, rs -> rs.getString(1))
                        .forEach(out::collect);
            }
        }
    }

    /**
     * 股权穿透
     */
    @Slf4j
    @RequiredArgsConstructor
    private static final class EquityBfsCalculator extends RichFlatMapFunction<String, Tuple2<String, List<Map<String, Object>>>> implements CheckpointedFunction {
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private final Config config;
        private EquityBfsService service;
        private Collector<Tuple2<String, List<Map<String, Object>>>> collector;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            service = new EquityBfsService();
        }

        @Override
        public void flatMap(String companyId, Collector<Tuple2<String, List<Map<String, Object>>>> out) {
            synchronized (bitmap) {
                if (collector == null) {
                    collector = out;
                }
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                }
                // 全量修复的时候, 来一条计算一条
                if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.REPAIR) {
                    flush(null);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush(context);
        }

        @Override
        public void close() {
            flush(null);
        }

        private void flush(FunctionSnapshotContext context) {
            synchronized (bitmap) {
                if (context != null) {
                    log.info("checkpoint id: {}, bitmap size: {}", context.getCheckpointId(), bitmap.getLongCardinality());
                }
                bitmap.iterator().forEachRemaining(this::consume);
                bitmap.clear();
            }
        }

        private void consume(Long companyId) {
            collector.collect(Tuple2.of(String.valueOf(companyId), service.bfs(companyId)));
        }
    }

    /**
     * 写入mysql
     */
    @RequiredArgsConstructor
    private static final class EquityBfsSink extends RichSinkFunction<Tuple2<String, List<Map<String, Object>>>> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_SOURCE);
            sink.enableCache();
        }

        @Override
        public void invoke(Tuple2<String, List<Map<String, Object>>> companyIdAndColumnMaps, Context context) {
            String companyId = companyIdAndColumnMaps.f0;
            List<Map<String, Object>> columnMaps = companyIdAndColumnMaps.f1;
            String sinkTable = String.format("%s_%s", SINK_TABLE, Long.parseLong(companyId) % 100);
            String deleteSql = new SQL()
                    .DELETE_FROM(sinkTable)
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            sink.update(deleteSql);
            for (Map<String, Object> columnMap : columnMaps) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String insertSql = new SQL()
                        .INSERT_INTO(sinkTable)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
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
            sink.flush();
        }
    }
}

