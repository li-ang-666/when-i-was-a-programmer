package com.liang.common.service.connector;

import com.liang.common.service.DaemonExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractCache<K, V> {
    // 公平锁
    private final Lock lock = new ReentrantLock(true);
    private final Map<K, Collection<V>> cache = new HashMap<>();
    private final KeySelector<K, V> keySelector;
    // cache
    private volatile int cacheMilliseconds;
    private volatile int cacheRecords;
    private volatile Thread sender;

    protected AbstractCache(int cacheMilliseconds, int cacheRecords, KeySelector<K, V> keySelector) {
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        this.keySelector = keySelector;
    }

    public final void enableCache() {
        enableCache(cacheMilliseconds, cacheRecords);
    }

    public final void enableCache(int cacheMilliseconds, int cacheRecords) {
        if (sender != null) return;
        this.cacheMilliseconds = cacheMilliseconds;
        this.cacheRecords = cacheRecords;
        sender = DaemonExecutor.launch("AbstractCacheThread", () -> {
            while (true) {
                // 先消耗掉可能存在的permit
                LockSupport.parkNanos(1);
                LockSupport.parkUntil(System.currentTimeMillis() + this.cacheMilliseconds);
                flush();
            }
        });
    }

    @SuppressWarnings("unchecked")
    public final void update(V... values) {
        if (values == null || values.length == 0) return;
        update(Arrays.asList(values));
    }

    public final void update(Collection<V> values) {
        // 拦截空值
        if (values == null || values.isEmpty()) return;
        try {
            // 同一批次的写入, 不拆开
            lock.lock();
            for (V value : values) {
                if (value == null) continue;
                K key = keySelector.selectKey(value);
                cache.compute(key, (k, v) -> {
                    Collection<V> buffer = (v != null) ? v : new ArrayList<>();
                    buffer.add(value);
                    // 有任意分区达到条数阈值, 唤醒sender
                    if (sender != null && buffer.size() >= cacheRecords) LockSupport.unpark(sender);
                    return buffer;
                });
            }
            if (sender == null) flush();
        } finally {
            lock.unlock();
        }
    }

    public final void flush() {
        try {
            lock.lock();
            for (Map.Entry<K, Collection<V>> entry : cache.entrySet()) {
                Collection<V> values = entry.getValue();
                if (values.isEmpty()) continue;
                K key = entry.getKey();
                updateImmediately(key, values);
                values.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void updateImmediately(K key, Collection<V> values);

    @FunctionalInterface
    protected interface KeySelector<K, V> {
        K selectKey(V v);
    }
}
