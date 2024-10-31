package com.liang.common.service.connector;

public class Timer {
    private long starTimestampMilliseconds;

    public void remake() {
        starTimestampMilliseconds = System.currentTimeMillis();
    }

    public long getInterval() {
        return System.currentTimeMillis() - starTimestampMilliseconds;
    }
}
