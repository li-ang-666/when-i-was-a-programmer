package com.liang.spark.job;

import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class TestJob {
    public static void main(String[] args) {
        JsonUtils.toString(new ArrayList<String>());
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.csv(spark, "tb.csv").show();
    }
}
