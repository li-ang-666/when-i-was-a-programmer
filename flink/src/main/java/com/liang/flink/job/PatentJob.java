package com.liang.flink.job;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.Distributor;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

@LocalConfigFile("patent.yml")
public class PatentJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> dataStream = StreamFactory.create(env);
        Distributor distributor = new Distributor();
        distributor
                .with("company_patent_basic_info_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("id")))
                .with("company_index", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("company_id")))
                .with("company_patent_basic_info_index_split", singleCanalBinlog -> String.valueOf(singleCanalBinlog.getColumnMap().get("company_id")));
        dataStream
                .rebalance()
                .flatMap(new PatentFlatMapper(config))
                .keyBy(distributor)
                .addSink(new PatentSink(config))
                .name("PatentSink")
                .uid("PatentSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("PatentJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class PatentFlatMapper extends RichFlatMapFunction<SingleCanalBinlog, SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate query;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            query = new JdbcTemplate("427.test");
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<SingleCanalBinlog> out) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if (table.equals("company_patent_basic_info")) {
                String id = (String) columnMap.get("id");
                String sql = new SQL().UPDATE("intellectual_property_info.company_patent_basic_info_index")
                        .SET("update_time = now()")
                        .WHERE("company_patent_info_id = " + SqlUtils.formatValue(id))
                        .toString();
                query.update(sql);
            } else {
                out.collect(singleCanalBinlog);
            }
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static final class PatentSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate query;
        private JdbcTemplate sink;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            query = new JdbcTemplate("427.test");
            sink = new JdbcTemplate("427.test");
            sink.enableCache();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            switch (singleCanalBinlog.getTable()) {
                case "company_patent_basic_info_index":
                    parseIndex(singleCanalBinlog);
                    break;
                case "company_index":
                case "company_patent_basic_info_index_split":
                    parseIndexSplit(singleCanalBinlog);
                    break;
                default:
                    break;
            }
        }

        private void parseIndex(SingleCanalBinlog singleCanalBinlog) {
            // delete
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String infoIndexId = String.valueOf(columnMap.get("id"));
            String deleteSql = new SQL().DELETE_FROM("company_patent_basic_info_index_split")
                    .WHERE("company_patent_basic_info_index_id = " + SqlUtils.formatValue(infoIndexId))
                    .toString();
            sink.update(deleteSql);
            String isDeleted = (String) columnMap.get("is_deleted");
            // insert
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE || !isDeleted.equals("0")) {
                return;
            }
            String infoId = String.valueOf(columnMap.get("company_patent_info_id"));
            String sql = new SQL().SELECT("patent_status")
                    .FROM("intellectual_property_info.company_patent_basic_info")
                    .WHERE("id = " + SqlUtils.formatValue(infoId))
                    .WHERE("is_deleted = 0")
                    .toString();
            String patentStatus = query.queryForObject(sql, rs -> rs.getString(1));
            // info表 和 index表 必须同时都有
            if (patentStatus == null) {
                return;
            }
            String patentType = String.valueOf(columnMap.get("patent_type"));
            String patentPublishYear = StrUtil.nullToDefault((String) columnMap.get("patent_publish_year"), "0");
            String patentApplicationYear = StrUtil.nullToDefault((String) columnMap.get("patent_application_year"), "0");
            String patentStatusDetail = StrUtil.blankToDefault((String) columnMap.get("patent_latest_legal_review_status"), "其他");
            String patentTitle = String.valueOf(columnMap.get("patent_title"));
            String patentApplicationNumber = String.valueOf(columnMap.get("patent_application_number"));
            String patentAnnounceNumber = String.valueOf(columnMap.get("patent_announce_number"));
            for (String companyId : CollUtil.newHashSet(String.valueOf(columnMap.get("company_ids")).split(";"))) {
                if (!TycUtils.isUnsignedId(companyId)) {
                    continue;
                }
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("company_patent_basic_info_index_id", infoIndexId);
                resultMap.put("company_patent_basic_info_id", infoId);
                resultMap.put("company_id", companyId);
                resultMap.put("patent_type", patentType);
                resultMap.put("patent_publish_year", patentPublishYear);
                resultMap.put("patent_application_year", patentApplicationYear);
                resultMap.put("patent_status", patentStatus);
                resultMap.put("patent_status_detail", patentStatusDetail);
                resultMap.put("patent_title", patentTitle);
                resultMap.put("patent_application_number", patentApplicationNumber);
                resultMap.put("patent_announce_number", patentAnnounceNumber);
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
                String insertSql = new SQL().INSERT_INTO("company_patent_basic_info_index_split")
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                sink.update(insertSql);
            }
        }

        private void parseIndexSplit(SingleCanalBinlog singleCanalBinlog) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String companyId = String.valueOf(columnMap.get("company_id"));
            String sql = new SQL()
                    .SELECT("patent_application_year", "patent_publish_year", "patent_type", "patent_status_detail")
                    .FROM("company_patent_basic_info_index_split")
                    .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                    .toString();
            Map<String, Map<String, Integer>> appYearStatisticMap = new TreeMap<>();
            Map<String, Map<String, Integer>> pubYearStatisticMap = new TreeMap<>();
            Map<String, Integer> typeStatisticMap = new TreeMap<>();
            Map<String, Integer> statusStatisticMap = new TreeMap<>();
            query.streamQuery(false, sql, rs -> {
                String appYear = rs.getString(1);
                String pubYear = rs.getString(2);
                String type = rs.getString(3);
                String status = rs.getString(4);
                appYearStatisticMap
                        .compute(appYear, (k, v) -> ObjUtil.defaultIfNull(v, new TreeMap<>()))
                        .compute(type, (k, v) -> ObjUtil.defaultIfNull(v, 0) + 1);
                pubYearStatisticMap
                        .compute(pubYear, (k, v) -> ObjUtil.defaultIfNull(v, new TreeMap<>()))
                        .compute(type, (k, v) -> ObjUtil.defaultIfNull(v, 0) + 1);
                typeStatisticMap
                        .compute(type, (k, v) -> ObjUtil.defaultIfNull(v, 0) + 1);
                statusStatisticMap
                        .compute(status, (k, v) -> ObjUtil.defaultIfNull(v, 0) + 1);
            });
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", companyId);
            resultMap.put("company_id", companyId);
            resultMap.put("app_year_statistic", JsonUtils.toString(complexMap2List(appYearStatisticMap)));
            resultMap.put("pub_year_statistic", JsonUtils.toString(complexMap2List(pubYearStatisticMap)));
            resultMap.put("type_statistic", JsonUtils.toString(simpleMap2List(typeStatisticMap)));
            resultMap.put("status_statistic", JsonUtils.toString(simpleMap2List(statusStatisticMap)));
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String insertSql = new SQL().REPLACE_INTO("company_patent_basic_info_index_split_statistic")
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString();
            sink.update(insertSql);
        }

        private List<Map<String, Object>> complexMap2List(Map<String, Map<String, Integer>> map) {
            return map.entrySet().stream()
                    .map(entry -> new LinkedHashMap<String, Object>() {{
                        put("label", entry.getKey());
                        put("num", entry.getValue().values().parallelStream().mapToInt(e -> e).sum());
                        put("stack", simpleMap2List(entry.getValue()));
                    }})
                    .collect(Collectors.toList());
        }

        private List<Map<String, Object>> simpleMap2List(Map<String, Integer> map) {
            return map.entrySet().stream()
                    .map(entry -> new LinkedHashMap<String, Object>() {{
                        put("label", entry.getKey());
                        put("num", entry.getValue());
                    }})
                    .collect(Collectors.toList());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            flush();
        }

        @Override
        public void finish() {
            flush();
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            sink.flush();
        }
    }
}
