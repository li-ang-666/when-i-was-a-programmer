package com.liang.tool.impl;

import cn.hutool.core.lang.Snowflake;
import com.liang.tool.service.ConfigHolder;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SnowIdTest extends ConfigHolder {

    public static void main(String[] args) {
        Set<Long> set = ConcurrentHashMap.newKeySet();
        Date date = new Date();
        Snowflake snowflake1 = new Snowflake(date, 1, 0, true);
        Snowflake snowflake2 = new Snowflake(date, 2, 0, true);
        Snowflake snowflake3 = new Snowflake(date, 3, 0, true);
        Snowflake snowflake4 = new Snowflake(date, 4, 0, true);
        Snowflake snowflake5 = new Snowflake(date, 5, 0, true);
        new Thread(() -> {
            while (true) {
                long l = snowflake1.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l1: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake2.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l2: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake3.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l3: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake4.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l4: {}", l);
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                long l = snowflake5.nextId();
                boolean add = set.add(l);
                if (!add) {
                    log.info("l5: {}", l);
                }
            }
        }).start();
    }
}
