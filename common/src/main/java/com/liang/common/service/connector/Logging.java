package com.liang.common.service.connector;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Logging {
    private final static int SLOW_MILLI = 3000;
    private final Timer timer = new Timer();
    private final String errorLog;
    private final String afterLog;

    public Logging(String classShortName, String instanceName) {
        String commonLog = String.format("[%s %s]", classShortName, instanceName);
        errorLog = commonLog + "{}({})";
        afterLog = commonLog + "{}({}) {}ms";
    }

    public void beforeExecute() {
        timer.remake();
    }

    public void ifError(String methodName, Object methodArg, Exception e) {
        log.error(errorLog, methodName, methodArg, e);
    }

    public void afterExecute(String methodName, Object methodArg) {
        afterExecute(methodName, methodArg, methodArg);
    }

    public void afterExecute(String methodName, Object debugMethodArg, Object warnMethodArg) {
        long interval = timer.getInterval();
        if (interval > SLOW_MILLI) {
            log.warn(afterLog, methodName, warnMethodArg, interval);
        } else if (log.isDebugEnabled()) {
            log.debug(afterLog, methodName, debugMethodArg, interval);
        }
    }

    private static final class Timer {
        private long starTimestampMilliseconds;

        public void remake() {
            starTimestampMilliseconds = System.currentTimeMillis();
        }

        public long getInterval() {
            return System.currentTimeMillis() - starTimestampMilliseconds;
        }
    }
}
