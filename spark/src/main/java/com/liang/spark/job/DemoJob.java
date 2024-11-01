package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.connector.ConnectorTester;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

public class DemoJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        Config config = ConfigUtils.getConfig();
        spark.table("flink.product_info")
                .limit(111)
                .repartition()
                .foreachPartition(new DemoSink(config));
        spark.close();
    }

    @RequiredArgsConstructor
    private static final class DemoSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            ConnectorTester connectorTester = new ConnectorTester();
            while (iterator.hasNext()) {
                Map<String, Object> columnMaps = JsonUtils.parseJsonObj(iterator.next().json());
                connectorTester.invoke();
            }
            connectorTester.flush();
        }
    }
}
