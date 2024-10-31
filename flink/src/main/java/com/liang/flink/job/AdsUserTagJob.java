package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class AdsUserTagJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        env
                .addSource(new AdsUserTagSource())
                .setParallelism(1)
                .name("AdsUserTagSource")
                .uid("AdsUserTagSource")
                .rebalance()
                .addSink(new AdsUserTagSink(ConfigUtils.getConfig()))
                .setParallelism(1)
                .name("AdsUserTagSink")
                .uid("AdsUserTagSink");
        env.execute("doris.ads_user_tag");
    }

    private final static class AdsUserTagSource implements SourceFunction<String> {
        private static final int INTERVAL = 1000 * 60;
        private final AtomicBoolean cancel = new AtomicBoolean(false);
        private long last = System.currentTimeMillis();

        @Override
        public void run(SourceContext<String> ctx) {
            while (!cancel.get()) {
                if (System.currentTimeMillis() - last >= INTERVAL) {
                    ctx.collect("");
                    last = System.currentTimeMillis();
                }
            }
        }

        @Override
        public void cancel() {
            cancel.set(true);
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class AdsUserTagSink extends RichSinkFunction<String> {
        private static final String INSERT_SQL_TEMPLATE = IoUtil.read(AdsUserTagSink.class.getClassLoader().getResourceAsStream("doris/ads_user_tag.sql"), StandardCharsets.UTF_8);
        private static final String INSERT_SQL_TEMPLATE_V2 = IoUtil.read(AdsUserTagSink.class.getClassLoader().getResourceAsStream("doris/ads_user_tag_v2.sql"), StandardCharsets.UTF_8);
        private final Config config;
        private JdbcTemplate doris;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            doris = new JdbcTemplate("doris");
        }

        @Override
        public void invoke(String value, Context context) {
            // ads.ads_user_tag
            String label = String.format("ads_ads_user_tag_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            String insertSql = String.format(INSERT_SQL_TEMPLATE, label);
            doris.update(insertSql);
            log.info("insert finish, see: `show load from ads where label = '{}'\\G` or `show transaction from ads where label = '{}'\\G`", label, label);
            // ads.ads_user_tag_v2
            String labelV2 = String.format("ads_ads_user_tag_v2_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            String insertSqlV2 = String.format(INSERT_SQL_TEMPLATE_V2, labelV2);
            doris.update(insertSqlV2);
            log.info("insert finish, see: `show load from ads where label = '{}'\\G` or `show transaction from ads where label = '{}'\\G`", labelV2, labelV2);
        }
    }
}
