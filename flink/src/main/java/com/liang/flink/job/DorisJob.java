package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.connector.database.template.doris.DorisWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.basic.kafka.KafkaSourceFactory;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

@Slf4j
@LocalConfigFile("doris/dwd_basic_data_collect_monitor_hours.yml")
public class DorisJob {
    private static final int CKP_INTERVAL = 1000 * 60;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        // 1分钟一次ckp
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(CKP_INTERVAL);
        checkpointConfig.setMinPauseBetweenCheckpoints(CKP_INTERVAL);
        Config config = ConfigUtils.getConfig();
        config.getDorisSchema().setUniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON);
        // chain
        DataStream<SingleCanalBinlog> stream;
        if (config.getKafkaConfigs().get("kafkaSource").getBootstrapServers().contains("ods")) {
            MapFunction<KafkaRecord<String>, SingleCanalBinlog> mapper = e -> {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(e.getValue());
                return new SingleCanalBinlog("", "", -1L, CanalEntry.EventType.INSERT, columnMap, columnMap);
            };
            stream = env.fromSource(KafkaSourceFactory.create(String::new), WatermarkStrategy.noWatermarks(), "KafkaSource")
                    .setParallelism(config.getFlinkConfig().getSourceParallel())
                    .name("KafkaSource")
                    .uid("KafkaSource")
                    .map(mapper)
                    .setParallelism(config.getFlinkConfig().getSourceParallel())
                    .name("OdsKafkaMapper")
                    .uid("OdsKafkaMapper");
        } else {
            stream = StreamFactory.create(env);
        }
        stream
                .rebalance()
                .addSink(new DorisSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("DorisSink")
                .uid("DorisSink");
        env.execute("doris." + config.getDorisSchema().getTableName());
    }

    @RequiredArgsConstructor
    private final static class DorisSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private DorisWriter dorisWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dorisWriter = new DorisWriter("dorisSink", 128 * 1024 * 1024);
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            columnMap.put(DorisSchema.DEFAULT_UNIQUE_DELETE_COLUMN, singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE ? 1 : 0);
            dorisWriter.write(new DorisOneRow(config.getDorisSchema(), columnMap));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            dorisWriter.flush();
        }

        @Override
        public void finish() {
            dorisWriter.flush();
        }

        @Override
        public void close() {
            dorisWriter.flush();
        }
    }
}
