package com.liang.common.service.connector.database.holder;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.connector.database.factory.DruidFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DruidHolder implements MultiPoolHolder<DruidDataSource> {
    private static final Map<String, DruidDataSource> POOLS = new ConcurrentHashMap<>();

    @Override
    public DruidDataSource getPool(String name) {
        return POOLS.computeIfAbsent(name,
                k -> new DruidFactory().createPool(k));
    }

    @Override
    public void closeAll() {
        POOLS.forEach((name, pool) -> {
            try {
                if (!pool.isClosed()) {
                    log.info("druid close {}", name);
                    pool.close();
                }
            } catch (Exception ignore) {
                log.warn("druid close {} error, ignore", name);
            }
        });
    }
}
