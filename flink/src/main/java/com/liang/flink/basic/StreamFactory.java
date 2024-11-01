package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateUtils;
import com.liang.common.util.StackUtils;
import com.liang.flink.basic.kafka.KafkaMonitor;
import com.liang.flink.basic.kafka.KafkaReporter;
import com.liang.flink.basic.kafka.KafkaSourceFactory;
import com.liang.flink.basic.repair.RepairGenerator;
import com.liang.flink.basic.repair.RepairHandler;
import com.liang.flink.basic.repair.RepairReporter;
import com.liang.flink.basic.repair.RepairSource;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static com.liang.common.dto.config.FlinkConfig.SourceType.KAFKA;

@Slf4j
@UtilityClass
@SuppressWarnings("deprecation")
public class StreamFactory {
    public static DataStream<SingleCanalBinlog> create(StreamExecutionEnvironment streamEnvironment) {
        Config config = ConfigUtils.getConfig();
        return config.getFlinkConfig().getSourceType() == KAFKA ?
                createKafkaStream(streamEnvironment) : createRepairStream(streamEnvironment);
    }

    private static DataStream<SingleCanalBinlog> createKafkaStream(StreamExecutionEnvironment streamEnvironment) {
        // 在JobManager启动汇报线程
        String jobClassName = StackUtils.getMainFrame().getClassName();
        String[] split = jobClassName.split("\\.");
        String simpleName = split[split.length - 1];
        String kafkaOffsetKey = String.format("%s_%s_%s", simpleName, "Offset", DateUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"));
        String kafkaTimeKey = String.format("%s_%s_%s", simpleName, "Time", DateUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"));
        log.info("kafkaOffsetKey: {}, kafkaTimeKey: {}", kafkaOffsetKey, kafkaTimeKey);
        DaemonExecutor.launch("KafkaReporter", new KafkaReporter(kafkaOffsetKey, kafkaTimeKey));
        // 组装KafkaSource
        Config config = ConfigUtils.getConfig();
        KafkaSource<KafkaRecord<BatchCanalBinlog>> kafkaSource = KafkaSourceFactory.create(BatchCanalBinlog::new);
        return streamEnvironment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .name("KafkaSource")
                .uid("KafkaSource")
                .setParallelism(config.getFlinkConfig().getSourceParallel())
                .flatMap(new KafkaMonitor(config, kafkaOffsetKey, kafkaTimeKey))
                .name("KafkaMonitor")
                .uid("KafkaMonitor")
                .setParallelism(1);
    }

    private static DataStream<SingleCanalBinlog> createRepairStream(StreamExecutionEnvironment streamEnvironment) {
        // 在JobManager启动汇报线程
        String jobClassName = StackUtils.getMainFrame().getClassName();
        String[] split = jobClassName.split("\\.");
        String simpleName = split[split.length - 1];
        String repairReportKey = String.format("%s_%s_%s", simpleName, "Report", DateUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"));
        log.info("repairReportKey: {}", repairReportKey);
        DaemonExecutor.launch("RepairReporter", new RepairReporter(repairReportKey));
        // 组装RepairSource
        Config config = ConfigUtils.getConfig();
        List<RepairTask> repairTasks = new RepairGenerator().generate();
        return streamEnvironment
                .addSource(new RepairSource(config, repairReportKey, repairTasks))
                .name("RepairSource")
                .uid("RepairSource")
                .setParallelism(1)
                .rebalance()
                .flatMap(new RepairHandler(config))
                .name("RepairHandler")
                .uid("RepairHandler")
                .setParallelism(config.getFlinkConfig().getSourceParallel());
    }
}
