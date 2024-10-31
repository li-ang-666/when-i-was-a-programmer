package com.liang.flink.basic.repair;

import com.liang.common.service.connector.database.template.RedisTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@RequiredArgsConstructor
public class RepairReporter implements Runnable {
    private final String repairKey;

    @Override
    public void run() {
        RedisTemplate redisTemplate = new RedisTemplate("metadata");
        while (true) {
            String logs = redisTemplate.lPop(repairKey);
            if (logs == null) {
                LockSupport.parkNanos(Duration.ofSeconds(3).toNanos());
            } else {
                if (logs.contains("() error")) {
                    log.error(logs);
                } else if (logs.contains("cancel()")) {
                    log.warn(logs);
                } else {
                    log.info(logs);
                }
            }
        }
    }
}
