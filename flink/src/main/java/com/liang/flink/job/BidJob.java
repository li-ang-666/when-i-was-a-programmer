package com.liang.flink.job;


import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.util.AreaCodeUtils;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.bid.BidService;
import com.liang.flink.service.LocalConfigFile;
import com.obs.services.ObsClient;
import lombok.RequiredArgsConstructor;
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

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@Slf4j
@LocalConfigFile("bid.yml")
public class BidJob {
    private static final List<String> COLUMNS = Arrays.asList(
            "id",
            "main_id",
            "bid_uuid",
            "bid_title",
            "bid_content",
            "bid_link",
            "bid_province",
            "bid_city",
            "public_info_lv1",
            "public_info_lv2",
            "bid_type",
            "bid_publish_time",
            "bid_item_num",
            "bid_contract_num",
            "purchaser",
            "proxy_unit",
            "bid_winner_info_json",
            "bid_winner",
            "winning_bid_amt_json",
            "winning_bid_amt_json_clean",
            "budget_amt_json",
            "budget_amt_json_clean",
            "bid_announcement_type",
            "create_time",
            "update_time",
            "is_dirty",
            "is_deleted"
    );
    private static final List<String> VOLCANIC_RDS = Arrays.asList(
            "volcanic_cloud_0",
            "volcanic_cloud_1",
            "volcanic_cloud_2",
            "volcanic_cloud_3",
            "volcanic_cloud_4",
            "volcanic_cloud_5",
            "volcanic_cloud_6",
            "volcanic_cloud_7",
            "volcanic_cloud_8",
            "volcanic_cloud_9",
            "volcanic_cloud_10"
    );
    private static final String SINK_RDS = "448.operating_info";
    private static final String SINK_TABlE = "company_bid_parsed_info";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .flatMap(new BidMapper(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidMapper")
                .uid("BidMapper")
                .keyBy(e -> (String) e.getColumnMap().get("id"))
                .addSink(new BidSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidSink")
                .uid("BidSink");
        env.execute("BidJob");
    }

    @RequiredArgsConstructor
    private static final class BidMapper extends RichFlatMapFunction<SingleCanalBinlog, SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate rds104;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            rds104 = new JdbcTemplate("104.data_bid");
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<SingleCanalBinlog> out) {
            if (singleCanalBinlog.getTable().equals("company_bid_info_v2")) {
                String sql = new SQL().SELECT("id")
                        .FROM("company_bid")
                        .WHERE("uuid = " + SqlUtils.formatValue(singleCanalBinlog.getColumnMap().get("bid_document_uuid")))
                        .toString();
                String id = rds104.queryForObject(sql, rs -> rs.getString(1));
                if (id == null) {
                    return;
                }
                singleCanalBinlog.getColumnMap().put("id", id);
            }
            out.collect(singleCanalBinlog);
        }
    }

    @RequiredArgsConstructor
    private static final class BidSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;
        private JdbcTemplate volcanic;
        private JdbcTemplate dataBid104;
        private JdbcTemplate rds069;
        private JdbcTemplate companyBase435;
        private BidService bidService;
        private ObsClient obsClient;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String volcanicRds = VOLCANIC_RDS.get(taskIdx % VOLCANIC_RDS.size());
//            volcanic = new JdbcTemplate(volcanicRds);
            volcanic = new JdbcTemplate("104.data_bid");
            dataBid104 = new JdbcTemplate("104.data_bid");
            rds069 = new JdbcTemplate("069.semantic_analysis");
            companyBase435 = new JdbcTemplate("435.company_base");
            bidService = new BidService();
            obsClient = new ObsWriter("").getClient();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            try {
                invokeWithE(singleCanalBinlog);
            } catch (Exception e) {
                log.error("error while process bid, id: {}", singleCanalBinlog.getColumnMap().get("id"), e);
            }
        }

        private void invokeWithE(SingleCanalBinlog singleCanalBinlog) {
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = (String) columnMap.get("id");
            // 查询company_bid
            String query104Sql = new SQL().SELECT("*")
                    .FROM("company_bid")
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            List<Map<String, Object>> companyBidColumnMaps = dataBid104.queryForColumnMaps(query104Sql);
            // 若不存在, 删除
            if (companyBidColumnMaps.isEmpty()) {
                String deleteSql = new SQL().DELETE_FROM(SINK_TABlE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
                return;
            }
            Map<String, Object> companyBidColumnMap = companyBidColumnMaps.get(0);
            // 整理company_bid相关数据
            String uuid = (String) companyBidColumnMap.get("uuid");
            resultMap.put("id", id);
            resultMap.put("bid_uuid", uuid);
            resultMap.put("main_id", id);
            resultMap.put("bid_title", companyBidColumnMap.get("title"));
            resultMap.put("bid_link", companyBidColumnMap.get("link"));
            String odsPublishTime = (String) companyBidColumnMap.get("publish_time");
            resultMap.put("bid_publish_time", odsPublishTime != null && !odsPublishTime.startsWith("0000") ? odsPublishTime : null);
            resultMap.put("is_deleted", companyBidColumnMap.get("deleted"));
            resultMap.put("bid_announcement_type", StrUtil.blankToDefault((String) companyBidColumnMap.get("type"), ""));
            resultMap.put("bid_content", putObs(uuid, (String) companyBidColumnMap.get("content")));
            // 查询算法表
            resultMap.put("is_dirty", "0");
            String query069Sql = new SQL().SELECT("*")
                    .FROM("company_bid_info_v2")
                    .WHERE("bid_document_uuid = " + SqlUtils.formatValue(uuid))
                    .toString();
            List<Map<String, Object>> rds069ColumnMaps = rds069.queryForColumnMaps(query069Sql);
            if (!rds069ColumnMaps.isEmpty()) {
                Map<String, Object> rds069ColumnMap = rds069ColumnMaps.get(0);
                // 整理算法表相关数据
                resultMap.put("public_info_lv1", rds069ColumnMap.get("first_class_info_type"));
                resultMap.put("public_info_lv2", rds069ColumnMap.get("secondary_info_type"));
                resultMap.put("bid_province", rds069ColumnMap.get("province"));
                resultMap.put("bid_city", rds069ColumnMap.get("city"));
                resultMap.putAll(bidService.parseBidInfo((String) rds069ColumnMap.get("bid_info")));
            }
            // 查询大模型
            String queryVolcanicSql = new SQL().SELECT("*")
                    .FROM("bid_tender_details")
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            List<Map<String, Object>> volcanicColumnMaps = volcanic.queryForColumnMaps(queryVolcanicSql);
            if (!volcanicColumnMaps.isEmpty()) {
                // 整理大模型相关数据
                Map<String, Object> volcanicColumnMap = volcanicColumnMaps.get(0);
                String lv1 = (String) volcanicColumnMap.get("announcement_type_first");
                if (isValid(lv1)) {
                    resultMap.put("public_info_lv1", lv1);
                }
                String lv2 = (String) volcanicColumnMap.get("announcement_type_second");
                if (isValid(lv2)) {
                    resultMap.put("public_info_lv2", lv2);
                }
                String province = AreaCodeUtils.getCode((String) volcanicColumnMap.get("project_province"));
                if (isValid(province)) {
                    resultMap.put("bid_province", province);
                }
                String city = AreaCodeUtils.getCode((String) volcanicColumnMap.get("project_city"));
                if (isValid(city)) {
                    resultMap.put("bid_city", city);
                }
                // 招标方 or 采购方
                String purchaser = (String) volcanicColumnMap.get("tender_info");
                if (StrUtil.startWith(purchaser, '[') && StrUtil.endWith(purchaser, ']')) {
                    String parsedPurchaser = parsePurchaser(purchaser);
                    if (!"[]".equals(parsedPurchaser)) {
                        resultMap.put("purchaser", "[" + parsedPurchaser + "]");
                    }
                }
                // 中标方 or 供应方
                String winner = (String) volcanicColumnMap.get("winning_bid_info");
                if (StrUtil.startWith(winner, '[') && StrUtil.endWith(winner, ']')) {
                    Tuple2<String, String> parsedWinner = parseWinner(winner);
                    if (!"[]".equals(parsedWinner.f0)) {
                        resultMap.put("bid_winner", "[" + parsedWinner.f0 + "]");
                    }
                    // 中标金额
                    if (!"[]".equals(parsedWinner.f1)) {
                        resultMap.put("winning_bid_amt_json_clean", "[" + parsedWinner.f1 + "]");
                    }
                }
            }
            write(resultMap);
        }

        private String putObs(String uuid, String content) {
            try {
                if (obsClient == null) {
                    obsClient = new ObsWriter("").getClient();
                }
                obsClient.putObject(
                        "jindi-bigdata",
                        "company_bid_parsed_info/content_obs_url/" + uuid + ".txt",
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
                );
                return "http://jindi-bigdata.obs.cn-north-4.myhuaweicloud.com/company_bid_parsed_info/content_obs_url/" + uuid + ".txt";
            } catch (Exception e) {
                obsClient = null;
                return putObs(uuid, content);
            }
        }

        private void write(Map<String, Object> resultMap) {
            for (String column : COLUMNS) {
                if (!resultMap.containsKey(column)) {
                    resultMap.put(column, "");
                }
            }
            resultMap.remove("create_time");
            resultMap.remove("update_time");
            String onDuplicateKeyUpdate = SqlUtils.onDuplicateKeyUpdate(COLUMNS);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().INSERT_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString() + onDuplicateKeyUpdate;
            sink.update(sql);
        }

        private String parsePurchaser(String json) {
            List<Object> list = JsonUtils.parseJsonArr(json);
            List<Map<String, Object>> maps = list.stream()
                    .map(map -> (String) (((Map<String, Object>) map).get("tender_organization")))
                    .filter(this::isValid)
                    .flatMap(names -> Stream.of(names.split("、")))
                    .filter(this::isValid)
                    .map(name -> new HashMap<String, Object>() {{
                                put("gid", queryCompanyId(name));
                                put("name", name);
                            }}
                    )
                    .collect(Collectors.toList());
            return JsonUtils.toString(maps);
        }

        private Tuple2<String, String> parseWinner(String json) {
            List<Object> list = JsonUtils.parseJsonArr(json);
            List<Map<String, Object>> maps1 = list.stream()
                    .map(map -> (String) (((Map<String, Object>) map).get("winning_bid_organization")))
                    .filter(this::isValid)
                    .flatMap(names -> Stream.of(names.split(";")))
                    .filter(this::isValid)
                    .map(name -> new HashMap<String, Object>() {{
                                put("gid", queryCompanyId(name));
                                put("name", name);
                            }}
                    )
                    .collect(Collectors.toList());
            List<Map<String, Object>> maps2 = ReUtil.findAllGroup0(Pattern.compile("\\d+(\\.\\d+)?万?元"), json)
                    .stream()
                    .map(money ->
                            new HashMap<String, Object>() {{
                                String moneyNumber = money.replaceAll("[万元]", "");
                                BigDecimal moneyDecimal;
                                try {
                                    moneyDecimal = new BigDecimal(moneyNumber);
                                } catch (Exception e) {
                                    log.warn("错误的金额: {}", money);
                                    moneyDecimal = new BigDecimal(0);
                                }
                                if (money.endsWith("万元")) {
                                    put("amount", moneyDecimal.multiply(new BigDecimal(10_000)).toPlainString());
                                } else {
                                    put("amount", moneyDecimal.toPlainString());
                                }
                            }}
                    ).collect(Collectors.toList());
            return Tuple2.of(JsonUtils.toString(maps1), JsonUtils.toString(maps2));
        }

        private String queryCompanyId(String companyName) {
            String sql = new SQL().SELECT("company_id")
                    .FROM("company_index")
                    .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                    .toString();
            String res = companyBase435.queryForObject(sql, rs -> rs.getString(1));
            return ObjUtil.defaultIfNull(res, "");
        }

        private boolean isValid(String str) {
            return StrUtil.isNotBlank(str) && !"无".equals(str);
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
