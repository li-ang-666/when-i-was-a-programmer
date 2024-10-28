package com.liang.spark.basic;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@UtilityClass
public class TableFactory {
    public static Dataset<Row> csv(SparkSession spark, String fileName) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("file:/Users/liang/Desktop/when-i-was-a-programmer/spark/src/main/resources/" + fileName);
    }
}
