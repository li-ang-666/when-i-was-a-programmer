package com.liang.flink.basic.kafka;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.BatchCanalBinlog;
import com.liang.flink.dto.KafkaRecord;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class KafkaMonitor extends RichFlatMapFunction<KafkaRecord<BatchCanalBinlog>, SingleCanalBinlog> {
    private static final int WRITE_REDIS_INTERVAL_MILLISECONDS = 1000 * 5;
    private static final String SEPARATOR = "\001";
    private final Map<String, String> offsetMap = new HashMap<>();
    private final Map<String, String> timeMap = new HashMap<>();
    private final Config config;
    private final String kafkaOffsetKey;
    private final String kafkaTimeKey;
    private long lastWriteTimeMillis = System.currentTimeMillis();
    private RedisTemplate redisTemplate;

    @Override
    public void open(Configuration parameters) {
        ConfigUtils.setConfig(config);
        redisTemplate = new RedisTemplate("metadata");
    }

    @Override
    public void flatMap(KafkaRecord<BatchCanalBinlog> kafkaRecord, Collector<SingleCanalBinlog> out) {
        String key = kafkaRecord.getTopic() + SEPARATOR + kafkaRecord.getPartition();
        offsetMap.put(key, String.valueOf(kafkaRecord.getOffset()));
        timeMap.put(key, String.valueOf(kafkaRecord.getReachMilliseconds() / 1000));
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastWriteTimeMillis >= WRITE_REDIS_INTERVAL_MILLISECONDS) {
            redisTemplate.hMSet(kafkaOffsetKey, offsetMap);
            redisTemplate.hMSet(kafkaTimeKey, timeMap);
            lastWriteTimeMillis = currentTimeMillis;
        }
        kafkaRecord.getValue().getSingleCanalBinlogs().forEach(out::collect);
    }

    @Override
    public void close() {
        redisTemplate.del(kafkaOffsetKey);
        redisTemplate.del(kafkaTimeKey);
    }
}
