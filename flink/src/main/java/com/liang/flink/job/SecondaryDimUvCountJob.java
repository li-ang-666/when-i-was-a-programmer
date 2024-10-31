package com.liang.flink.job;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

public class SecondaryDimUvCountJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"secondary-dim-uv-count.yml"};
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        env.setParallelism(1);
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.addSink(new Sink(ConfigUtils.getConfig()));
        env.execute("SecondaryDimUvCountJob");
    }

    private static class Sink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("bigdataOnline");
        }

        @Override
        public void invoke(SingleCanalBinlog value, Context context) throws Exception {
            CanalEntry.EventType eventType = value.getEventType();
            Map<String, Object> columnMap = value.getColumnMap();
            String secondaryDimName = String.valueOf(columnMap.get("visit_secondary_dim_name"));
            if (eventType != CanalEntry.EventType.DELETE && "最终受益人".equals(secondaryDimName)) {
                // unique (visit_page_name, visit_secondary_dim_name, visit_date)
                String visitDate = String.valueOf(columnMap.get("visit_date"));
                String visitPageName = String.valueOf(columnMap.get("visit_page_name"));
                String deleteSQL = String.format("delete from itch_point_secondary_dims_uv_count where visit_date = '%s' and visit_page_name = '%s' and visit_secondary_dim_name = '受益所有人'", visitDate, visitPageName);
                jdbcTemplate.update(deleteSQL);
                String visitCount = String.valueOf(columnMap.get("visit_count"));
                String insertSQL = String.format("insert into itch_point_secondary_dims_uv_count(id,visit_date,visit_page_name,visit_secondary_dim_name,visit_count,create_time,update_time)" +
                        "values(DEFAULT,'%s','%s','受益所有人','%s',DEFAULT,DEFAULT)", visitDate, visitPageName, visitCount);
                jdbcTemplate.update(insertSQL);
            }
        }
    }
}