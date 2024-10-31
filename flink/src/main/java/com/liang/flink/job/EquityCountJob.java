package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.HbaseTemplate;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

@LocalConfigFile("equity-control-count.yml")
public class EquityCountJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream
                .rebalance()
                .flatMap(new EquityCountFlatMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityCountFlatMapper")
                .uid("EquityCountFlatMapper")
                .keyBy(e -> e)
                .addSink(new EquityCountSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("EquityCountSink")
                .uid("EquityCountSink");
        env.execute("EquityCountJob");
    }

    @RequiredArgsConstructor
    private static final class EquityCountFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            // entity_controller_details_new
            out.collect(String.valueOf(columnMap.get("tyc_unique_entity_id")));
            out.collect(String.valueOf(columnMap.get("company_id_controlled")));
            // company_index
            out.collect(String.valueOf(columnMap.get("company_id")));
            // human
            out.collect(String.valueOf(columnMap.get("human_id")));
        }
    }

    @RequiredArgsConstructor
    private static final class EquityCountSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate jdbcTemplate457;
        private JdbcTemplate jdbcTemplate463;
        private HbaseTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate457 = new JdbcTemplate("457.prism_shareholder_path");
            jdbcTemplate463 = new JdbcTemplate("463.bdp_equity");
            sink = new HbaseTemplate("hbaseSink");
            sink.enableCache();
        }

        @Override
        public void invoke(String tycUniqueEntityId, Context context) {
            if (!TycUtils.isTycUniqueEntityId(tycUniqueEntityId)) return;
            HbaseOneRow hbaseOneRow;
            if (TycUtils.isUnsignedId(tycUniqueEntityId)) {
                hbaseOneRow = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, tycUniqueEntityId)
                        // 公司详情页-受益人count
                        //.put("has_beneficiary", queryHasBeneficiary(tycUniqueEntityId))

                        // 公司详情页-实控人count
                        .put("has_controller", queryHasController(tycUniqueEntityId))
                        // 公司详情页-实控权count
                        .put("num_control_ability", queryNumControlAbility(tycUniqueEntityId))
                ;
            } else {
                hbaseOneRow = new HbaseOneRow(HbaseSchema.HUMAN_ALL_COUNT, tycUniqueEntityId)
                        // 老板详情页-受益权count
                        //.put("num_benefit_ability", queryNumBenefitAbility(tycUniqueEntityId))

                        // 老板详情页-实控权count
                        .put("num_control_ability", queryNumControlAbility(tycUniqueEntityId))
                ;
            }
            sink.update(hbaseOneRow);
        }

        // 查询实控人count
        private String queryHasController(String tycUniqueEntityId) {
            String sql = new SQL()
                    .SELECT("count(1)")
                    .FROM("entity_controller_details_new")
                    .WHERE("company_id_controlled = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .WHERE("is_controller_tyc_unique_entity_id = 1")
                    .toString();
            return jdbcTemplate463.queryForObject(sql, rs -> rs.getString(1));
        }

        // 查询实控权count
        private String queryNumControlAbility(String tycUniqueEntityId) {
            String sql = new SQL()
                    .SELECT("count(1)")
                    .FROM("entity_controller_details_new")
                    .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .toString();
            return jdbcTemplate463.queryForObject(sql, rs -> rs.getString(1));
        }

        // 查询受益权count
        private String queryNumBenefitAbility(String tycUniqueEntityId) {
            String sql = new SQL()
                    .SELECT("count(1)")
                    .FROM("entity_beneficiary_details_new")
                    .WHERE("tyc_unique_entity_id_beneficial = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .WHERE("is_beneficiary = 1")
                    .WHERE("entity_special_type != 1")
                    .toString();
            return jdbcTemplate463.queryForObject(sql, rs -> rs.getString(1));
        }

        // 查询受益人count
        private String queryHasBeneficiary(String tycUniqueEntityId) {
            String sql = new SQL()
                    .SELECT("count(1)")
                    .FROM("ratio_path_company")
                    .WHERE("company_id = " + SqlUtils.formatValue(tycUniqueEntityId))
                    .WHERE("is_ultimate = 1")
                    .toString();
            return jdbcTemplate457.queryForObject(sql, rs -> rs.getString(1));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
