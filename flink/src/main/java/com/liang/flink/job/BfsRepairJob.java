package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BfsRepairJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(null);
        Config config = ConfigUtils.getConfig();
        env.addSource(new BfsRepairSource())
                .setParallelism(1)
                .name("BfsRepairSource")
                .uid("BfsRepairSource")
                .rebalance()
                .addSink(new BfsRepairSink(config))
                .setParallelism(1)
                .name("BfsRepairSink")
                .uid("BfsRepairSink");
        env.execute("BfsRepairJob");
    }

    private static final class BfsRepairSource extends RichSourceFunction<String> {
        private final AtomicBoolean canceled = new AtomicBoolean(false);

        @Override
        public void run(SourceContext<String> ctx) {
            int i = 0;
            InputStream resourceAsStream = BfsRepairJob.class.getClassLoader().getResourceAsStream("wrong-company-ids.txt");
            String[] companyIds = IoUtil.readUtf8(resourceAsStream).split("\n");
            while (!canceled.get() && i < companyIds.length) {
                ctx.collect(companyIds[i++].replaceAll(".*?(\\d+).*", "$1"));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception ignore) {
                }
            }
            cancel();
        }

        @Override
        public void cancel() {
            canceled.set(true);
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class BfsRepairSink extends RichSinkFunction<String> {
        private final Config config;
        private JdbcTemplate graphData430;
        private JdbcTemplate prism116;
        private JdbcTemplate prismShareholderPath491;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            graphData430 = new JdbcTemplate("430.graph_data");
            prism116 = new JdbcTemplate("116.prism");
            prismShareholderPath491 = new JdbcTemplate("491.prism_shareholder_path");
        }

        @Override
        public void invoke(String companyId, Context context) {
            if (!TycUtils.isUnsignedId(companyId)) {
                return;
            }
            log.info("{}", companyId);
            String sql = new SQL().UPDATE("company_equity_relation_details")
                    .SET("update_time = now()")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .LIMIT(1)
                    .toString();
            graphData430.update(sql);
        }
    }
}
