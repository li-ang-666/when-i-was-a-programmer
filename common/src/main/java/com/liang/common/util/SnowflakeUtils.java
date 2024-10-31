package com.liang.common.util;

import cn.hutool.core.lang.Snowflake;
import com.liang.common.service.connector.database.template.RedisTemplate;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
@UtilityClass
public class SnowflakeUtils {
    // 从即日起,可以使用(2^41 / 1000 / 3600 / 24 / 365)大概69年
    private final static Date START_DATE = new Date(DateUtils.unixTimestamp("2023-01-01 00:00:00") * 1000L);
    // 最多 2^31 个机房
    private final static long MAX_DATA_CENTER_NUM = 32L;
    // 每个机房最多 2^31 个机器
    private final static long MAX_WORKER_NUM = 32L;
    // 使用第三方类包来代替System.currentTimeMillis(), 可以避免操作系统时间回退
    private final static boolean USE_THIRD_CLOCK = true;
    // 不允许时间回退
    private final static long ALLOW_BACK_MILLI = 0L;
    // 低频时(毫秒内无冲突) SEQUENCE_MASK 随机值的上界
    private final static long LOW_FREQUENCY_SEQUENCE_MASK_LIMIT = 7L;
    // 单例
    private static volatile Snowflake SNOWFLAKE;

    /**
     * 需要 Redis 支持
     */
    public static void init(String JobName) {
        if (SNOWFLAKE == null) {
            synchronized (SnowflakeUtils.class) {
                if (SNOWFLAKE == null) {
                    String lockKey = JobName + "Lock";
                    String incrKey = JobName + "Incr";
                    RedisTemplate redisTemplate = new RedisTemplate("metadata");
                    while (!redisTemplate.tryLock(lockKey)) {
                    }
                    long incr = (redisTemplate.incr(incrKey) - 1) % (MAX_WORKER_NUM * MAX_DATA_CENTER_NUM);
                    long dataCenterId = incr / MAX_DATA_CENTER_NUM;
                    long workerId = incr % MAX_WORKER_NUM;
                    redisTemplate.unlock(lockKey);
                    log.info("Snowflake init, dataCenterId: {}, workerId: {}", dataCenterId, workerId);
                    SNOWFLAKE = new Snowflake(START_DATE, workerId, dataCenterId, USE_THIRD_CLOCK, ALLOW_BACK_MILLI, LOW_FREQUENCY_SEQUENCE_MASK_LIMIT);
                }
            }
        }
    }

    public static Long nextId() {
        return SNOWFLAKE.nextId();
    }
}
