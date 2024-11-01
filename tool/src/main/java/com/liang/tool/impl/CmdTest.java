package com.liang.tool.impl;

import cn.hutool.core.util.RuntimeUtil;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

public class CmdTest {
    public static void main(String[] args) {
        RuntimeUtil
                .execForLines("yarn application -list | grep RUNNING")
                .stream()
                .map(e -> Arrays.asList(e.split("\\s+")))
                .filter(e -> e.contains("liang"))
                .map(e -> Tuple2.of(e.get(0), e.get(1)))
                .forEach(System.out::println);

    }

}
