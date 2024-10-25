package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class TestJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.csv(spark, "tb.csv").show();
    }
}
