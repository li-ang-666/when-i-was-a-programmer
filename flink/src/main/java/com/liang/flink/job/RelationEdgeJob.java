package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@LocalConfigFile("relation-edge.yml")
public class RelationEdgeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .flatMap(new RelationEdgeMapper(config))
                .name("RelationEdgeMapper")
                .uid("RelationEdgeMapper")
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(e -> e)
                .addSink(new RelationEdgeSink(config))
                .name("RelationEdgeSink")
                .uid("RelationEdgeSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationEdgeJob");
    }

    private enum Relation {
        LEGAL, HIS_LEGAL, AC, HIS_INVEST, INVEST, BRANCH
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeMapper extends RichFlatMapFunction<SingleCanalBinlog, String> {
        private final Config config;
        private final Map<String, String> dictionary = new HashMap<>();

        {
            dictionary.put("company_index", "company_id");
            dictionary.put("company_legal_person", "company_id");
            dictionary.put("company_equity_relation_details", "company_id");
            dictionary.put("company_branch", "company_id");
            dictionary.put("entity_controller_details_new", "company_id_controlled");
            dictionary.put("entity_investment_history_fusion_details", "company_id_invested");
            dictionary.put("entity_legal_rep_list_total", "tyc_unique_entity_id");
            dictionary.put("company_human_relation", "company_graph_id");
            dictionary.put("entity_empty_index", "graph_id");
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<String> out) {
            String key = dictionary.getOrDefault(singleCanalBinlog.getTable(), null);
            if (key != null) {
                Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
                if (!beforeColumnMap.isEmpty()) {
                    out.collect(StrUtil.nullToDefault((String) beforeColumnMap.get(key), ""));
                }
                Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
                if (!afterColumnMap.isEmpty()) {
                    out.collect(StrUtil.nullToDefault((String) afterColumnMap.get(key), ""));
                }
            }
        }
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeSink extends RichSinkFunction<String> implements CheckpointedFunction {
        private static final String SINK_SOURCE = "466.bdp_personnel";
        private static final String SINK_TABLE = "relation_edge";
        private final Config config;
        private final Map<String, String> dictionary = new HashMap<>();
        private final Roaring64Bitmap bitmap = new Roaring64Bitmap();
        private JdbcTemplate prismBoss157;
        private JdbcTemplate companyBase435;
        private JdbcTemplate bdpEquity463;
        private JdbcTemplate bdpPersonnel466;
        private JdbcTemplate graphData430;
        private JdbcTemplate sink;
        private JdbcTemplate sinkQuery;

        {
            dictionary.put("1", "法定代表人");
            dictionary.put("2", "负责人");
            dictionary.put("3", "经营者");
            dictionary.put("4", "投资人");
            dictionary.put("5", "执行事务合伙人");
            dictionary.put("6", "法定代表人|负责人");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            prismBoss157 = new JdbcTemplate("157.prism_boss");
            companyBase435 = new JdbcTemplate("435.company_base");
            bdpEquity463 = new JdbcTemplate("463.bdp_equity");
            bdpPersonnel466 = new JdbcTemplate("466.bdp_personnel");
            graphData430 = new JdbcTemplate("430.graph_data");
            sink = new JdbcTemplate(SINK_SOURCE);
            sink.enableCache();
            sinkQuery = new JdbcTemplate(SINK_SOURCE);
        }

        @Override
        public void invoke(String companyId, Context context) {
            synchronized (bitmap) {
                if (TycUtils.isUnsignedId(companyId)) {
                    bitmap.add(Long.parseLong(companyId));
                    if (config.getFlinkConfig().getSourceType() == FlinkConfig.SourceType.REPAIR) {
                        flush(null);
                    }
                }
            }
        }

        private void flush(Long ckpId) {
            synchronized (bitmap) {
                if (config.getFlinkConfig().getSourceType() != FlinkConfig.SourceType.REPAIR && ckpId != null) {
                    log.info("checkpoint-{}, bitmap size: {}", ckpId, bitmap.getLongCardinality());
                }
                bitmap.forEach(companyId -> {
                    String targetId = String.valueOf(companyId);
                    String companyName = queryCompanyName(targetId);
                    // 非法公司 删除
                    if (!TycUtils.isValidName(companyName)) {
                        String deleteSql = new SQL().DELETE_FROM(SINK_TABLE)
                                .WHERE("target_id = " + SqlUtils.formatValue(targetId))
                                .toString();
                        sink.update(deleteSql);
                    }
                    // 合法公司 diff
                    else {
                        // 制造冗余数据 也先删除
                        sinkQuery.update(new SQL().DELETE_FROM(SINK_TABLE)
                                .WHERE("target_id = " + SqlUtils.formatValue(targetId))
                                .toString());
                        ArrayList<Row> results = new ArrayList<>();
                        parseLegalPerson(targetId, companyName, results);
                        parseController(targetId, companyName, results);
                        parseShareholder(targetId, companyName, results);
                        parseBranch(targetId, companyName, results);
                        parseHisShareholder(targetId, companyName, results);
                        parseHisLegalPerson(targetId, companyName, results);
                        Set<Map<String, Object>> newColumnMaps = results.stream()
                                .filter(Row::isValid)
                                .map(Row::toColumnMap)
                                .collect(Collectors.toSet());
                        Set<Map<String, Object>> oldColumnMaps = new HashSet<>(sinkQuery.queryForColumnMaps(new SQL()
                                .SELECT("source_id", "source_name", "target_id", "target_name", "relation", "other")
                                .FROM(SINK_TABLE)
                                .WHERE("target_id = " + SqlUtils.formatValue(companyId))
                                .toString()));
                        for (Map<String, Object> oldColumnMap : oldColumnMaps) {
                            if (!newColumnMaps.contains(oldColumnMap)) {
                                String where = SqlUtils.columnMap2Where(oldColumnMap);
                                String deleteSql = new SQL().DELETE_FROM(SINK_TABLE)
                                        .WHERE(where)
                                        .toString();
                                sink.update(deleteSql);
                            }
                        }
                        for (Map<String, Object> newColumnMap : newColumnMaps) {
                            if (!oldColumnMaps.contains(newColumnMap)) {
                                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(newColumnMap);
                                String insertSql = new SQL().INSERT_INTO(SINK_TABLE)
                                        .INTO_COLUMNS(insert.f0)
                                        .INTO_VALUES(insert.f1)
                                        .toString();
                                sink.update(insertSql);
                            }
                        }
                    }
                });
                bitmap.clear();
                sink.flush();
            }
        }

        // 法人 -> 公司
        private void parseLegalPerson(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_legal_person")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("legal_rep_type");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = "1".equals(type) ? (String) columnMap.get("legal_rep_name_id") : (String) columnMap.get("legal_rep_human_id");
                String sourceName = (String) columnMap.get("legal_rep_name");
                String other = (String) columnMap.get("legal_rep_display_name");
                results.add(new Row(sourceId, sourceName, companyId, companyName, Relation.LEGAL, other));
            }
        }

        // 实控人 -> 公司
        private void parseController(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_controller_details_new")
                    .WHERE("company_id_controlled = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_controller_tyc_unique_entity_id = 1")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpEquity463.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String sourceId = (String) columnMap.get("tyc_unique_entity_id");
                String sourceName = (String) columnMap.get("entity_name_valid");
                String other = "";
                results.add(new Row(sourceId, sourceName, companyId, companyName, Relation.AC, other));
            }
        }

        // 股东 -> 公司
        private void parseShareholder(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_equity_relation_details")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            List<Map<String, Object>> columnMaps = graphData430.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("shareholder_type");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = (String) columnMap.get("shareholder_id");
                String name = (String) columnMap.get("shareholder_name");
                String other = (String) columnMap.get("equity_ratio");
                if (StrUtil.isNotBlank(other)) {
                    other = new BigDecimal(other).setScale(12, RoundingMode.DOWN).toPlainString();
                }
                results.add(new Row(sourceId, name, companyId, companyName, Relation.INVEST, other));
            }
        }

        // 分公司 -> 总公司
        private void parseBranch(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("company_branch")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_deleted = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String sourceId = (String) columnMap.get("branch_company_id");
                String name = queryCompanyName(sourceId);
                String other = "";
                results.add(new Row(sourceId, name, companyId, companyName, Relation.BRANCH, other));
            }
        }

        // 历史股东 -> 公司
        private void parseHisShareholder(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_investment_history_fusion_details")
                    .WHERE("company_id_invested = " + SqlUtils.formatValue(companyId))
                    .WHERE("delete_status = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpEquity463.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("entity_type_id");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceNameId = (String) columnMap.get("entity_name_id");
                String sourceId = "1".equals(type) ? sourceNameId : queryPid(companyId, sourceNameId);
                String name = (String) columnMap.get("entity_name_valid");
                String other = StrUtil.nullToDefault((String) columnMap.get("investment_ratio"), "");
                if (StrUtil.isNotBlank(other)) {
                    other = new BigDecimal(other).setScale(12, RoundingMode.DOWN).toPlainString();
                }
                results.add(new Row(sourceId, name, companyId, companyName, Relation.HIS_INVEST, other));
            }
        }

        // 历史法人 -> 公司
        private void parseHisLegalPerson(String companyId, String companyName, List<Row> results) {
            String sql = new SQL().SELECT("*")
                    .FROM("entity_legal_rep_list_total")
                    .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyId))
                    .WHERE("is_history_legal_rep = 1")
                    .WHERE("delete_status = 0")
                    .toString();
            List<Map<String, Object>> columnMaps = bdpPersonnel466.queryForColumnMaps(sql);
            for (Map<String, Object> columnMap : columnMaps) {
                String type = (String) columnMap.get("entity_type_id_legal_rep");
                if ("3".equals(type)) {
                    continue;
                }
                String sourceId = (String) columnMap.get("tyc_unique_entity_id_legal_rep");
                String name = (String) columnMap.get("entity_name_valid_legal_rep");
                String other = dictionary.get((String) columnMap.get("legal_rep_type_display_name"));
                results.add(new Row(sourceId, name, companyId, companyName, Relation.HIS_LEGAL, other));
            }
        }

        private String queryPid(String companyGid, String humanGid) {
            String sql = new SQL().SELECT("human_pid")
                    .FROM("company_human_relation")
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyGid))
                    .WHERE("human_graph_id = " + SqlUtils.formatValue(humanGid))
                    .WHERE("deleted = 0")
                    .toString();
            return prismBoss157.queryForObject(sql, rs -> rs.getString(1));
        }

        private String queryCompanyName(String companyGid) {
            String sql = new SQL().SELECT("company_name")
                    .FROM("company_index")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyGid))
                    .toString();
            return companyBase435.queryForObject(sql, rs -> rs.getString(1));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush(context.getCheckpointId());
        }

        @Override
        public void finish() {
            flush(null);
        }

        @Override
        public void close() {
            flush(null);
        }
    }

    @Data
    @AllArgsConstructor
    @Accessors(chain = true)
    private static final class Row implements Serializable {
        private String sourceId;
        private String sourceName;
        private String targetId;
        private String targetName;
        private Relation relation;
        private String other;

        public boolean isValid() {
            return TycUtils.isTycUniqueEntityId(sourceId)
                    && TycUtils.isValidName(sourceName)
                    && TycUtils.isUnsignedId(targetId);
        }

        public Map<String, Object> toColumnMap() {
            return new LinkedHashMap<String, Object>() {{
                put("source_id", sourceId);
                put("source_name", sourceName);
                put("target_id", targetId);
                put("target_name", targetName);
                put("relation", relation.toString());
                put("other", other);
            }};
        }
    }
}
