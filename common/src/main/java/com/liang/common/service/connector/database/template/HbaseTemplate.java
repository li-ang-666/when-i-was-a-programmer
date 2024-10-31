package com.liang.common.service.connector.database.template;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.connector.AbstractCache;
import com.liang.common.service.connector.Logging;
import com.liang.common.service.connector.database.holder.HbaseConnectionHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 列出所有表:
 * list
 * <p>
 * 列出某个namespace下的表:
 * list_namespace_tables '${namespace}'
 * <p>
 * 建表:
 * create '${namespace}:${tableName}','${family}'
 * <p>
 * 查看所有row:
 * scan '${namespace}:${tableName}',{FORMATTER => 'toString'}
 * <p>
 * 查看某个row:
 * get '${namespace}:${tableName}','${rowKey}',{FORMATTER => 'toString'}
 * <p>
 * 清表:
 * truncate '${namespace}:${tableName}'
 * <p>
 * 删表:
 * disable '${namespace}:${tableName}';drop '${namespace}:${tableName}'
 */
@Slf4j
public class HbaseTemplate extends AbstractCache<HbaseSchema, HbaseOneRow> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 3000;
    private final static int DEFAULT_CACHE_RECORDS = 1024;
    private final Connection pool;
    private final Logging logging;

    public HbaseTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, HbaseOneRow::getSchema);
        pool = new HbaseConnectionHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    @Override
    protected void updateImmediately(HbaseSchema schema, Collection<HbaseOneRow> hbaseOneRows) {
        logging.beforeExecute();
        try (Table table = getTable(schema)) {
            List<Put> puts = new ArrayList<>();
            for (HbaseOneRow hbaseOneRow : hbaseOneRows) {
                Put put = new Put(Bytes.toBytes(hbaseOneRow.getRowKey()));
                for (Map.Entry<String, Object> entry : hbaseOneRow.getColumnMap().entrySet()) {
                    String col = entry.getKey();
                    String value = String.valueOf(entry.getValue());
                    byte[] valueArr = value.equalsIgnoreCase("null") ? null : Bytes.toBytes(value);
                    put.addColumn(Bytes.toBytes(hbaseOneRow.getSchema().getColumnFamily()),
                            Bytes.toBytes(col), valueArr);
                }
                puts.add(put);
            }
            // todo: 源码调用了table.batch()方法,好像可以同时包含put和delete
            table.put(puts);
            Object methodArg = hbaseOneRows.size() > 100 ? hbaseOneRows.size() + "条" : hbaseOneRows;
            logging.afterExecute("upsert", methodArg);
        } catch (Exception e) {
            logging.ifError("upsert", hbaseOneRows, e);
        }
    }

    public HbaseOneRow getRow(HbaseOneRow hbaseOneRow) {
        logging.beforeExecute();
        HbaseSchema schema = hbaseOneRow.getSchema();
        String rowKey = hbaseOneRow.getRowKey();
        HbaseOneRow resultOneRow = SerializationUtils.clone(hbaseOneRow);
        try (Table table = getTable(schema)) {
            Get get = new Get(Bytes.toBytes(rowKey))
                    .setCacheBlocks(false)
                    .addFamily(Bytes.toBytes(schema.getColumnFamily()));
            // 每个result是一行
            Result result = table.get(get);
            if (result != null) {
                for (Cell cell : result.listCells()) {
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    resultOneRow.put(column, value);
                }
            }
            logging.afterExecute("getRow", hbaseOneRow);
            return resultOneRow;
        } catch (Exception e) {
            logging.ifError("getRow", hbaseOneRow, e);
            return resultOneRow;
        }
    }

    public void scan(HbaseSchema schema, HbaseOneRowConsumer consumer) {
        logging.beforeExecute();
        try (Table table = getTable(schema);
             ResultScanner scanner = table.getScanner(new Scan()
                     .addFamily(Bytes.toBytes(schema.getColumnFamily()))
                     .setCacheBlocks(false)
                     .setCaching(1024))) {
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                HbaseOneRow resultOneRow = new HbaseOneRow(schema, rowKey);
                for (Cell cell : result.listCells()) {
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    resultOneRow.put(column, value);
                }
                consumer.consume(resultOneRow);
            }
            logging.afterExecute("scan", schema);
        } catch (Exception e) {
            logging.ifError("scan", schema, e);
        }
    }

    private Table getTable(HbaseSchema schema) throws Exception {
        return pool.getTable(TableName.valueOf(
                schema.getNamespace(),
                schema.getTableName()));
    }

    @FunctionalInterface
    public interface HbaseOneRowConsumer {
        void consume(HbaseOneRow hbaseOneRow);
    }
}
