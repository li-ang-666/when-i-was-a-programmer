package com.liang.repair.test;

import static com.liang.common.util.DateUtils.*;

public class DateUtilsTest {
    public static void main(String[] args) {
        System.out.println(currentDate());
        System.out.println(currentDatetime());
        System.out.println("---");
        System.out.println(fromUnixTime(System.currentTimeMillis() / 1000));
        System.out.println(fromUnixTime(System.currentTimeMillis() / 1000, "yyyy-MM-dd"));
        System.out.println(fromUnixTime(System.currentTimeMillis() / 1000, "yyyy-MM-dd HH:mm:ss"));
        System.out.println(fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMdd"));
        System.out.println(fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMddHHmmss"));
        System.out.println("---");
        System.out.println(unixTimestamp("2024-01-01"));
        System.out.println(unixTimestamp("2024-01-01 00:00:00"));
        System.out.println(unixTimestamp("2024-01-01", "yyyy-MM-dd"));
        System.out.println(unixTimestamp("2024-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss"));
        System.out.println(unixTimestamp("20240101", "yyyyMMdd"));
        System.out.println(unixTimestamp("20240101000000", "yyyyMMddHHmmss"));
        System.out.println("---");
        System.out.println(dateFormat("2024-01-01", "yyyy_MM_dd HH_mm_ss"));
        System.out.println(dateFormat("2024-01-01 00:00:00", "yyyy_MM_dd HH_mm_ss"));
        System.out.println(dateFormat("2024-01-01", "yyyy-MM-dd", "yyyy_MM_dd HH_mm_ss"));
        System.out.println(dateFormat("2024-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "yyyy_MM_dd HH_mm_ss"));
        System.out.println(dateFormat("20240101", "yyyyMMdd", "yyyy_MM_dd HH_mm_ss"));
        System.out.println(dateFormat("20240101000000", "yyyyMMddHHmmss", "yyyy_MM_dd HH_mm_ss"));
        System.out.println("---");
        System.out.println(dateAdd("2024-01-01", 1));
        System.out.println(dateAdd("2024-01-01 00:00:00", 1));
        System.out.println(dateAdd("2024-01-01 12:12:12", 1));
        System.out.println(dateAdd("2024-01-01", -1));
        System.out.println(dateAdd("2024-01-01 00:00:00", -1));
        System.out.println(dateAdd("2024-01-01 12:12:12", -1));
        System.out.println("---");
        System.out.println(getOfflinePt(1, "yyyyMMdd"));
        System.out.println(getOfflinePt(1, "yyyy-MM-dd HH:mm:ss"));
        System.out.println(getOfflinePt(-1, "yyyyMMdd"));
        System.out.println(getOfflinePt(-1, "yyyy-MM-dd HH:mm:ss"));
    }
}
