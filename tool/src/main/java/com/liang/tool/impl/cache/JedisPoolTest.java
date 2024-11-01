package com.liang.tool.impl.cache;

import com.liang.common.service.connector.database.template.RedisTemplate;
import com.liang.tool.service.ConfigHolder;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public class JedisPoolTest extends ConfigHolder {
    public static void main(String[] args) {
        RedisTemplate redisTemplate = new RedisTemplate("metadata");
        redisTemplate.rPush("list_test_3", "a");
        redisTemplate.rPush("list_test_3", "b");
        redisTemplate.rPush("list_test_3", "b");

        while (true) {
            System.out.println(redisTemplate.lPop("list_test_3"));
            LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
        }
    }

}
