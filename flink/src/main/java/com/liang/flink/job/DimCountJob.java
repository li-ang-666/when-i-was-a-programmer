package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.connector.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.impl.RatioPathCompany;
import com.liang.flink.service.LocalConfigFile;
import com.liang.flink.service.data.update.DataUpdateContext;
import com.liang.flink.service.data.update.DataUpdateImpl;
import com.liang.flink.service.data.update.DataUpdateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

@Slf4j
@LocalConfigFile("dim-count.yml")
public class DimCountJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        DataStream<SingleCanalBinlog> sourceStream = StreamFactory.create(env);
        Config config = ConfigUtils.getConfig();
        sourceStream
                .rebalance()
                .addSink(new DimCountSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("DimCountSink");
        env.execute("DimCountJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    @DataUpdateImpl({
            RatioPathCompany.class
    })
    private final static class DimCountSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private DataUpdateService<HbaseOneRow> service;
        private HbaseTemplate hbaseTemplate;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            DataUpdateContext<HbaseOneRow> context = new DataUpdateContext<>(DimCountSink.class);
            service = new DataUpdateService<>(context);
            hbaseTemplate = new HbaseTemplate("hbaseSink");
            hbaseTemplate.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            List<HbaseOneRow> hbaseRows = service.invoke(singleCanalBinlog);
            hbaseTemplate.update(hbaseRows);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            hbaseTemplate.flush();
        }

        @Override
        public void finish() {
            hbaseTemplate.flush();
        }

        @Override
        public void close() {
            hbaseTemplate.flush();
            ConfigUtils.unloadAll();
        }
    }
}
