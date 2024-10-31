package com.liang.common.service.connector.database.holder;

import com.liang.common.service.connector.database.factory.HbaseConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HbaseConnectionHolder implements MultiPoolHolder<Connection> {
    private static final Map<String, Connection> POOLS = new ConcurrentHashMap<>();

    @Override
    public Connection getPool(String name) {
        return POOLS.computeIfAbsent(name,
                k -> new HbaseConnectionFactory().createPool(k));
    }

    @Override
    public void closeAll() {
        POOLS.forEach((name, pool) -> {
            try {
                if (!pool.isClosed()) {
                    log.info("hbase close {}", name);
                    pool.close();
                }
            } catch (Exception ignore) {
                log.warn("hbase close {} error, ignore", name);
            }
        });
    }
}
