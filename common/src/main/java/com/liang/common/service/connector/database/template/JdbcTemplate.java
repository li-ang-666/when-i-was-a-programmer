package com.liang.common.service.connector.database.template;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.service.connector.AbstractCache;
import com.liang.common.service.connector.Logging;
import com.liang.common.service.connector.database.holder.DruidHolder;
import com.liang.common.util.DorisBitmapUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class JdbcTemplate extends AbstractCache<String, String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 3000;
    private final static int DEFAULT_CACHE_RECORDS = 128;
    private final static String BITMAP_COLUMN_NAME = "bitmap";
    private final DruidDataSource pool;
    private final Logging logging;

    public JdbcTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, sql -> "");
        pool = new DruidHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    @Override
    protected void updateImmediately(String ignore, Collection<String> sqls) {
        boolean getException = false;
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                for (String sql : sqls) {
                    statement.addBatch(sql);
                }
                statement.executeBatch();
                connection.commit();
                logging.afterExecute("updateBatch", sqls, sqls.size() + "条");
            }
        } catch (Exception e) {
            // 归还的时候, DruidDataSource.recycle 会自动 rollback 一次
            // 批量更新报错不打印明细, 单条执行报错再打印具体SQL
            logging.ifError("updateBatch", sqls.size() + "条", e);
            getException = true;
        }
        if (getException) {
            for (String sql : sqls) {
                LockSupport.parkUntil(System.currentTimeMillis() + 100);
                int retryTimes = 0;
                int maxRetryTimes = 3;
                while (++retryTimes <= maxRetryTimes) {
                    String failedLogPrefix = "/* 第" + retryTimes + "次重试 */";
                    logging.beforeExecute();
                    try (DruidPooledConnection connection = pool.getConnection()) {
                        connection.setAutoCommit(true);
                        try (Statement statement = connection.createStatement()) {
                            statement.executeUpdate(sql);
                            logging.afterExecute("updateSingle", failedLogPrefix + sql);
                            // 打断循环
                            retryTimes += maxRetryTimes;
                        }
                    } catch (Exception ee) {
                        String methodArg = (retryTimes == maxRetryTimes)
                                ? ("/* " + ee.getClass().getName() + ": " + ee.getMessage() + " */" + " " + failedLogPrefix + sql)
                                : (failedLogPrefix + sql);
                        logging.ifError("updateSingle", methodArg, ee);
                        LockSupport.parkUntil(System.currentTimeMillis() + 100);
                    }
                }
            }
        }
    }

    public <T> T queryForObject(String sql, ResultSetMapper<T> resultSetMapper) {
        if (StringUtils.isBlank(sql)) {
            return null;
        }
        logging.beforeExecute();
        ArrayList<T> list = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    list.add(resultSet.next() ? resultSetMapper.map(resultSet) : null);
                    logging.afterExecute("queryForObject", sql);
                    return list.get(0);
                }
            }
        } catch (Exception e) {
            logging.ifError("queryForObject", sql, e);
            return null;
        }
    }

    public <T> List<T> queryForList(String sql, ResultSetMapper<T> resultSetMapper) {
        ArrayList<T> list = new ArrayList<>();
        if (StringUtils.isBlank(sql)) {
            return list;
        }
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        list.add(resultSetMapper.map(resultSet));
                    }
                    logging.afterExecute("queryForList", sql);
                    return list;
                }
            }
        } catch (Exception e) {
            logging.ifError("queryForList", sql, e);
            return list;
        }
    }

    public List<Map<String, Object>> queryForColumnMaps(String sql) {
        logging.beforeExecute();
        List<Map<String, Object>> result = new ArrayList<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (resultSet.next()) {
                        HashMap<String, Object> columnMap = new LinkedHashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object columnValue = BITMAP_COLUMN_NAME.equals(columnName) ?
                                    DorisBitmapUtils.parseBinary(resultSet.getBytes(i)) : resultSet.getString(i);
                            columnMap.put(columnName, columnValue);
                        }
                        result.add(columnMap);
                    }
                    logging.afterExecute("queryForColumnMaps", sql);
                    return result;
                }
            }
        } catch (Exception e) {
            logging.ifError("queryForColumnMaps", sql, e);
            return result;
        }
    }

    public Map<String, Object> queryForColumnMap(String sql) {
        logging.beforeExecute();
        Map<String, Object> columnMap = new LinkedHashMap<>();
        try (DruidPooledConnection connection = pool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    if (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object columnValue = BITMAP_COLUMN_NAME.equals(columnName) ?
                                    DorisBitmapUtils.parseBinary(resultSet.getBytes(i)) : resultSet.getString(i);
                            columnMap.put(columnName, columnValue);
                        }
                    }
                    logging.afterExecute("queryForColumnMap", sql);
                    return columnMap;
                }
            }
        } catch (Exception e) {
            logging.ifError("queryForColumnMap", sql, e);
            return columnMap;
        }
    }

    public void streamQuery(boolean ifThrow, String sql, ResultSetConsumer consumer) {
        logging.beforeExecute();
        try (DruidPooledConnection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                statement.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        consumer.consume(resultSet);
                    }
                }
            }
            logging.afterExecute("streamQuery", sql);
        } catch (Exception e) {
            logging.ifError("streamQuery", sql, e);
            if (ifThrow) {
                throw new RuntimeException("streamQuery error", e);
            }
        }
    }

    public Connection getConnection() throws Exception {
        return pool.getConnection();
    }

    @FunctionalInterface
    public interface ResultSetMapper<T> extends Serializable {
        T map(ResultSet rs) throws Exception;
    }

    @FunctionalInterface
    public interface ResultSetConsumer extends Serializable {
        void consume(ResultSet rs) throws Exception;
    }
}
