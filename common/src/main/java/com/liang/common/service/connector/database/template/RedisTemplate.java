package com.liang.common.service.connector.database.template;

import com.liang.common.service.connector.Logging;
import com.liang.common.service.connector.database.holder.JedisPoolHolder;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

@Slf4j
public class RedisTemplate {
    private final JedisPool pool;
    private final Logging logging;

    public RedisTemplate(String name) {
        pool = new JedisPoolHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    public Long incr(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            Long res = jedis.incr(key);
            logging.afterExecute("incr", key);
            return res;
        } catch (Exception e) {
            logging.ifError("incr", key, e);
            return null;
        }
    }

    public String get(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            String res = jedis.get(key);
            logging.afterExecute("get", key);
            return res;
        } catch (Exception e) {
            logging.ifError("get", key, e);
            return null;
        }
    }

    public void set(String key, String value) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            jedis.set(key, value);
            logging.afterExecute("set", key + " -> " + value);
        } catch (Exception e) {
            logging.ifError("set", key + " -> " + value, e);
        }
    }

    public void del(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
            logging.afterExecute("del", key);
        } catch (Exception e) {
            logging.ifError("del", key, e);
        }
    }

    public void hSet(String key, String field, String value) {
        hMSet(key, Collections.singletonMap(field, value));
    }

    public void hMSet(String key, Map<String, String> map) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            if (!map.isEmpty()) jedis.hmset(key, map);
            logging.afterExecute("hMSet", key + " -> " + map);
        } catch (Exception e) {
            logging.ifError("hMSet", key + " -> " + map, e);
        }
    }

    /**
     * <p>127.0.0.1:6379> HSCAN ${key} ${cursor} [MATCH pattern] [COUNT count]
     * <p>HSCAN 命令每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为 HSCAN 命令的游标参数, 以此来延续之前的迭代
     * <p>当 SCAN 命令的游标参数被设置为 0 时, 服务器将开始一次新的迭代, 而当服务器向用户返回值为 0 的游标时, 表示迭代已结束
     */

    public Map<String, String> hScan(String key) {
        logging.beforeExecute();
        Map<String, String> result = new HashMap<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START; //其实就是 "0"
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key, cursor, scanParams);
                cursor = scanResult.getCursor();
                scanResult.getResult().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
            } while (!"0".equals(cursor));
            logging.afterExecute("hScan", key);
            return result;
        } catch (Exception e) {
            logging.ifError("hScan", key, e);
            return result;
        }
    }

    public List<String> scan() {
        logging.beforeExecute();
        List<String> result = new ArrayList<>();
        try (Jedis jedis = pool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                result.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
            logging.afterExecute("scan", "");
            return result;
        } catch (Exception e) {
            logging.ifError("scan", "", e);
            return result;
        }
    }

    public boolean tryLock(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            Long reply = jedis.setnx(key, "lock");
            jedis.pexpireAt(key, System.currentTimeMillis() + 500);
            logging.afterExecute("tryLock", key);
            return reply == 1;
        } catch (Exception e) {
            logging.ifError("tryLock", key, e);
            return false;
        }
    }

    public void unlock(String key) {
        del(key);
    }

    // addLast()
    public void rPush(String key, String value) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            jedis.rpush(key, value);
            logging.afterExecute("rPush", key + " -> " + value);
        } catch (Exception e) {
            logging.ifError("rPush", key + " -> " + value, e);
        }
    }

    // getFirst()
    public String lPop(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            logging.afterExecute("lPop", key);
            return jedis.lpop(key);
        } catch (Exception e) {
            logging.ifError("lPop", key, e);
            return null;
        }
    }

    public void setBit(String key, int offset, boolean value) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            logging.afterExecute("setBit", key + " -> " + offset + " -> " + value);
            jedis.setbit(key, offset, value);
        } catch (Exception e) {
            logging.ifError("setBit", key, e);
        }
    }

    public int bitCount(String key) {
        logging.beforeExecute();
        try (Jedis jedis = pool.getResource()) {
            logging.afterExecute("bitCount", key);
            return (int) ((long) jedis.bitcount(key));
        } catch (Exception e) {
            logging.ifError("bitCount", key, e);
            return -1;
        }
    }

    public Jedis getJedis() {
        return pool.getResource();
    }
}
