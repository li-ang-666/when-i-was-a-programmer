package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.DaemonExecutor;
import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.kafka.KafkaSourceFactory;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/*
 * drop table if exists flink.open_api_record;
 * create external table if not exists flink.open_api_record (
 *   org_name string,
 *   order_code string,
 *   token string,
 *   interface_id string,
 *   interface_name string,
 *   billing_rules string,
 *   request_ip string,
 *   request_timestamp string,
 *   request_datetime string,
 *   response_timestamp string,
 *   response_datetime string,
 *   cost string,
 *   error_code string,
 *   error_message string,
 *   charge_status string,
 *   return_status string,
 *   params string
 * )
 * partitioned by(request_date string)
 * ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 * STORED AS TEXTFILE;
 *
 * MSCK REPAIR TABLE flink.open_api_record;
 */
@Slf4j
@LocalConfigFile("open-api-record-job.yml")
public class OpenApiRecordJob {
    // hive jdbc
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final int PARTITION_FLUSH_INTERVAL_MILLI = 1000 * 60;
    // obs
    private static final ZoneOffset ZONE_OFFSET = ZoneOffset.of("+8");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String DEFAULT_DATETIME = "0000-00-00 00:00:00.000";
    private static final String DIR = "obs://hadoop-obs/hive/warehouse/%s.db/%s/request_date=%s/";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        final String DATABASE = (String) config.getOtherConfigs().get("database");
        final String TABLE = (String) config.getOtherConfigs().get("table");
        log.info("DATABASE: {}, TABLE: {}, DIR: {}", DATABASE, TABLE, String.format(DIR, DATABASE, TABLE, "pt"));
        KafkaSource<KafkaRecord<String>> kafkaSource = KafkaSourceFactory.create(String::new);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .name("KafkaSource")
                .uid("KafkaSource")
                .setParallelism(1)
                .rebalance()
                .addSink(new OpenApiRecordSink(config))
                .name("OpenApiRecordSink")
                .uid("OpenApiRecordSink")
                .setParallelism(1);
        DaemonExecutor.launch("PartitionRepairer", () -> {
            try {
                Class.forName(DRIVER);
                while (true) {
                    try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
                        connection.prepareStatement("MSCK REPAIR TABLE " + DATABASE + "." + TABLE).executeUpdate();
                    } catch (Exception e) {
                        log.warn("MSCK REPAIR TABLE ERROR", e);
                    }
                    LockSupport.parkUntil(System.currentTimeMillis() + PARTITION_FLUSH_INTERVAL_MILLI);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        env.execute("OpenApiRecordJob");
    }

    @RequiredArgsConstructor
    private static final class OpenApiRecordSink extends RichSinkFunction<KafkaRecord<String>> implements CheckpointedFunction {
        private final Map<String, ObsWriter> pt2ObsWriter = new HashMap<>();
        private final Config config;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void invoke(KafkaRecord<String> kafkaRecord, Context context) {
            final String DATABASE = (String) config.getOtherConfigs().get("database");
            final String TABLE = (String) config.getOtherConfigs().get("table");
            // read map
            String columnJson = kafkaRecord.getValue();
            Map<String, Object> columnMap = JsonUtils.parseJsonObj(columnJson);
            if (columnMap == null) {
                log.warn("unresolvable json: {}, topic: {}, offset: {}", columnJson, kafkaRecord.getTopic(), kafkaRecord.getOffset());
                return;
            }
            String orgName = String.valueOf(columnMap.get("orgName"));
            String orderCode = String.valueOf(columnMap.get("orderCode"));
            String token = String.valueOf(columnMap.get("token"));
            String interfaceId = String.valueOf(columnMap.get("interfaceId"));
            String interfaceName = String.valueOf(columnMap.get("interfaceName"));
            String billingRules = String.valueOf(columnMap.get("billingRules"));
            String requestIp = String.valueOf(columnMap.get("requestIp"));
            // 调用时间
            String requestTimestamp = String.valueOf(columnMap.get("requestTimestamp"));
            String requestDatetime = timestamp2Datetime(requestTimestamp);
            // 返回时间
            String responseTimestamp = String.valueOf(columnMap.get("responseTimestamp"));
            String responseDatetime = timestamp2Datetime(responseTimestamp);
            String cost = String.valueOf(columnMap.get("cost"));
            String errorCode = String.valueOf(columnMap.get("errorCode"));
            String errorMessage = String.valueOf(columnMap.get("errorMessage"));
            String chargeStatus = String.valueOf(columnMap.get("chargeStatus"));
            String returnStatus = String.valueOf(columnMap.get("returnStatus"));
            String params = String.valueOf(columnMap.get("params"));
            // write map
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("org_name", orgName);
            resultMap.put("order_code", orderCode);
            resultMap.put("token", token);
            resultMap.put("interface_id", interfaceId);
            resultMap.put("interface_name", interfaceName);
            resultMap.put("billing_rules", billingRules);
            resultMap.put("request_ip", requestIp);
            resultMap.put("request_timestamp", requestTimestamp);
            resultMap.put("request_datetime", requestDatetime);
            resultMap.put("response_timestamp", responseTimestamp);
            resultMap.put("response_datetime", responseDatetime);
            resultMap.put("cost", cost);
            resultMap.put("error_code", errorCode);
            resultMap.put("error_message", errorMessage);
            resultMap.put("charge_status", chargeStatus);
            resultMap.put("return_status", returnStatus);
            resultMap.put("params", params);
            // pt
            String pt = requestDatetime.substring(0, 10);
            String targetDir = String.format(DIR, DATABASE, TABLE, pt);
            // write
            synchronized (pt2ObsWriter) {
                pt2ObsWriter
                        .compute(targetDir, (dir, existedObsWriter) -> {
                            ObsWriter obsWriter = (existedObsWriter != null) ? existedObsWriter : new ObsWriter(targetDir, ObsWriter.FileFormat.TXT);
                            obsWriter.enableCache();
                            return obsWriter;
                        })
                        .update(JsonUtils.toString(resultMap));
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

        private String timestamp2Datetime(String timestamp) {
            String datetime = DEFAULT_DATETIME;
            try {
                long timestampLong = Long.parseLong(timestamp);
                datetime = LocalDateTime.ofEpochSecond(timestampLong / 1000, 0, ZONE_OFFSET).format(FORMATTER) + "." + timestampLong % 1000;
            } catch (Exception ignore) {
            }
            return datetime;
        }

        private void flush() {
            synchronized (pt2ObsWriter) {
                pt2ObsWriter.forEach((dir, ObsWriter) -> ObsWriter.flush());
                pt2ObsWriter.entrySet().removeIf(entry -> {
                    String dir = entry.getKey();
                    String requestDate = dir.replaceAll(".*?/request_date=(.*?)/.*", "$1");
                    String yesterday = LocalDateTime.now().plusDays(-1).format(FORMATTER);
                    return requestDate.compareTo(yesterday) < 0;
                });
            }
        }
    }
}
