package com.liang.flink.job;

import cn.hutool.core.util.ObjUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Lists;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
@LocalConfigFile("company-bid-parsed-info.yml")
public class CompanyBidParsedInfoJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new CompanyBidParsedInfoSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("CompanyBidParsedInfoSink")
                .uid("CompanyBidParsedInfoSink");
        env.execute("CompanyBidParsedInfoJob");
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyBidParsedInfoSink extends RichSinkFunction<SingleCanalBinlog> {
        private static final String SINK_TABLE = "company_bid_parsed_info";
        private final Config config;
        private JdbcTemplate source;
        private JdbcTemplate sink;
        private JdbcTemplate companyBase435;
        private JdbcTemplate semanticAnalysis069;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            source = new JdbcTemplate("427.test");
            sink = new JdbcTemplate("427.test");
            companyBase435 = new JdbcTemplate("435.company_base");
            semanticAnalysis069 = new JdbcTemplate("069.semantic_analysis");
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            // read map
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            String uuid = String.valueOf(columnMap.get("uuid"));
            // delete mysql
            if (singleCanalBinlog.getEventType() == CanalEntry.EventType.DELETE) {
                String deleteSql = new SQL()
                        .DELETE_FROM(SINK_TABLE)
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                sink.update(deleteSql);
            }
            Map<String, Object> resultMap = new HashMap<>();
            //
            String postResult;
            if (columnMap.containsKey("post_result")) {
                postResult = String.valueOf(columnMap.get("post_result"));
            } else {
                String querySql = new SQL()
                        .SELECT("post_result")
                        .FROM("company_bid_plus")
                        .WHERE("id = " + SqlUtils.formatValue(id))
                        .toString();
                String queryResult = source.queryForObject(querySql, rs -> rs.getString(1));
                postResult = (queryResult != null) ? queryResult : "{}";
            }
            Map<String, Object> postResultColumnMap = parseJson(postResult);
        }

        //@Override
        //public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
        //    Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        //    String bidInfo = String.valueOf(columnMap.get("bid_info"));
        //    Map<String, Object> parsedColumnMap = parseBidInfo(bidInfo);
        //    for (Map.Entry<String, Object> entry : parsedColumnMap.entrySet()) {
        //        String key = entry.getKey();
        //        Object value = entry.getValue();
        //        System.out.println(key + " -> " + value);
        //    }
        //}

        private Map<String, Object> parseJson(String json) {
            Map<String, Object> columnMap = new LinkedHashMap<>();
            Map<String, Object> result = Optional.ofNullable(json)
                    .map(JsonUtils::parseJsonObj)
                    .map(e -> e.get("result"))
                    .map(e -> (Map<String, Object>) e)
                    .orElseGet(HashMap::new);
            // simple column
            columnMap.put("bid_province", result.getOrDefault("province", ""));
            columnMap.put("bid_city", result.getOrDefault("city", ""));
            columnMap.put("public_info_lv1", result.getOrDefault("first_class_info_type", ""));
            columnMap.put("public_info_lv2", result.getOrDefault("secondary_info_type", ""));
            columnMap.put("bid_type", result.getOrDefault("bid_type", ""));
            columnMap.put("is_dirty", result.getOrDefault("is_dirty", "0"));
            columnMap.putAll(parseBidInfo(String.valueOf(result.getOrDefault("bid_info", "[]"))));
            return columnMap;
        }

        private Map<String, Object> parseBidInfo(String bidInfo) {
            Map<String, Object> columnMap = new LinkedHashMap<>();
            // prepare
            List<String> itemNos = Lists.newArrayList();
            List<String> contractNos = Lists.newArrayList();
            List<Map<String, Object>> purchasers = Lists.newArrayList();
            List<Map<String, Object>> candidates = Lists.newArrayList();
            List<Map<String, Object>> winners = Lists.newArrayList();
            List<Map<String, Object>> winnerRawAmounts = Lists.newArrayList();
            List<Map<String, Object>> winnerAmounts = Lists.newArrayList();
            List<Map<String, Object>> budgetRawAmounts = Lists.newArrayList();
            List<Map<String, Object>> budgetAmounts = Lists.newArrayList();
            // parse
            List<Object> parsedObjects = ObjUtil.defaultIfNull(JsonUtils.parseJsonArr(bidInfo), new ArrayList<>());
            for (Object parsedObject : parsedObjects) {
                Map<String, Object> parsedMap = (Map<String, Object>) parsedObject;
                // item_no
                itemNos.add(String.valueOf(parsedMap.getOrDefault("item_no", "")));
                // contract_no
                contractNos.add(String.valueOf(parsedMap.getOrDefault("contract_no", "")));
                // action
                List<Map<String, Object>> actionMaps = (List<Map<String, Object>>) (parsedMap.getOrDefault("action", new ArrayList<>()));
                for (Map<String, Object> actionMap : actionMaps) {
                    // raw budget
                    String rawBalance = String.valueOf(actionMap.getOrDefault("raw_balance", ""));
                    // 预算, 不为空就保留
                    if (TycUtils.isValidName(rawBalance)) {
                        budgetRawAmounts.add(new HashMap<String, Object>() {{
                            put("amount", rawBalance);
                        }});
                    }
                    // budget
                    String balance = String.valueOf(actionMap.getOrDefault("balance", ""));
                    // 预算, 不为空就保留
                    if (TycUtils.isValidName(balance)) {
                        budgetAmounts.add(new HashMap<String, Object>() {{
                            put("amount", balance);
                        }});
                    }
                    // purchaser
                    List<Map<String, Object>> clientMaps = (List<Map<String, Object>>) (actionMap.getOrDefault("client", new ArrayList<>()));
                    for (Map<String, Object> clientMap : clientMaps) {
                        String name = String.valueOf(clientMap.getOrDefault("name", ""));
                        // 只要名字正确, 就保留
                        if (TycUtils.isValidName(name)) {
                            purchasers.add(new LinkedHashMap<String, Object>() {{
                                put("gid", queryCompanyIdByCompanyName(name));
                                put("name", name);
                            }});
                        }
                    }
                    // candidate
                    Map<String, Object> candidateMap = (Map<String, Object>) (actionMap.getOrDefault("supplier_candidate", new LinkedHashMap<>()));
                    List<Map<String, Object>> candidateEntities = (List<Map<String, Object>>) (candidateMap.getOrDefault("entity", new ArrayList<>()));
                    for (Map<String, Object> candidateEntity : candidateEntities) {
                        String name = String.valueOf(candidateEntity.getOrDefault("name", ""));
                        // 只要名字正确, 就保留
                        if (TycUtils.isValidName(name)) {
                            candidates.add(new LinkedHashMap<String, Object>() {{
                                put("gid", queryCompanyIdByCompanyName(name));
                                put("name", name);
                                put("raw_offer_price", String.valueOf(candidateEntity.getOrDefault("raw_offer_price", "")));
                                put("offer_price", String.valueOf(candidateEntity.getOrDefault("offer_price", "")));
                            }});
                        }
                    }
                    // winner
                    Map<String, Object> winnerMap = (Map<String, Object>) (actionMap.getOrDefault("supplier", new LinkedHashMap<>()));
                    List<Map<String, Object>> winnerEntities = (List<Map<String, Object>>) (winnerMap.getOrDefault("entity", new ArrayList<>()));
                    for (Map<String, Object> winnerEntity : winnerEntities) {
                        String name = String.valueOf(winnerEntity.getOrDefault("name", ""));
                        // 只要名字正确, 就保留
                        if (TycUtils.isValidName(name)) {
                            // winner
                            winners.add(new LinkedHashMap<String, Object>() {{
                                put("gid", queryCompanyIdByCompanyName(name));
                                put("name", name);
                            }});
                            // raw winner amount
                            winnerRawAmounts.add(new HashMap<String, Object>() {{
                                put("amount", String.valueOf(winnerEntity.getOrDefault("raw_offer_price", "")));
                            }});
                            // winner amount
                            winnerAmounts.add(new HashMap<String, Object>() {{
                                put("amount", String.valueOf(winnerEntity.getOrDefault("offer_price", "")));
                            }});
                        }
                    }
                }
            }
            columnMap.put("item_no", itemNos.isEmpty() ? "" : itemNos.get(0));
            columnMap.put("contract_no", contractNos.isEmpty() ? "" : contractNos.get(0));
            columnMap.put("purchasers", purchasers.isEmpty() ? "" : JsonUtils.toString(purchasers));
            columnMap.put("candidates", candidates.isEmpty() ? "" : JsonUtils.toString(candidates));
            columnMap.put("winners", winners.isEmpty() ? "" : JsonUtils.toString(winners));
            columnMap.put("winner_raw_amounts", winnerRawAmounts.isEmpty() ? "" : JsonUtils.toString(winnerRawAmounts));
            columnMap.put("winner_amounts", winnerAmounts.isEmpty() ? "" : JsonUtils.toString(winnerAmounts));
            columnMap.put("budget_raw_amounts", budgetRawAmounts.isEmpty() ? "" : JsonUtils.toString(budgetRawAmounts));
            columnMap.put("budget_amounts", budgetAmounts.isEmpty() ? "" : JsonUtils.toString(budgetAmounts));
            return columnMap;
        }

        private String queryCompanyIdByCompanyName(String companyName) {
            String sql = new SQL()
                    .SELECT("company_id")
                    .FROM("company_index")
                    .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                    .toString();
            String res = companyBase435.queryForObject(sql, rs -> rs.getString(1));
            return ObjUtil.defaultIfNull(res, "");
        }
    }
}