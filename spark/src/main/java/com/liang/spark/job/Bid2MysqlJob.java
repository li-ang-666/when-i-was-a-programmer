package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

public class Bid2MysqlJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        Config config = ConfigUtils.getConfig();
        spark.table("test.company_bid_mismatch_abs")
                .repartition()
                .foreachPartition(new Bid2MysqlSink("company_bid_mismatch_abs", config));
        spark.table("test.company_bid_mismatch_might")
                .repartition()
                .foreachPartition(new Bid2MysqlSink("company_bid_mismatch_might", config));
        spark.close();
    }

    @RequiredArgsConstructor
    private static final class Bid2MysqlSink implements ForeachPartitionFunction<Row> {
        private final String sinkTable;
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            JdbcTemplate source = new JdbcTemplate("104.data_bid");
            JdbcTemplate sink = new JdbcTemplate("volcanic_cloud_0");
            sink.enableCache();
            while (iterator.hasNext()) {
                Map<String, Object> rowMap = JsonUtils.parseJsonObj(iterator.next().json());
                String querySql = new SQL().SELECT("id,uuid,title,content,type,deleted")
                        .FROM("company_bid")
                        .WHERE("id = " + SqlUtils.formatValue(rowMap.get("id")))
                        .toString();
                Map<String, Object> columnMap = source.queryForColumnMap(querySql);
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String insertSql = new SQL().INSERT_INTO(sinkTable)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
            }
            sink.flush();
        }
    }
}
