package com.liang.flink.project.bid;

import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
public class BidService {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");

    public Map<String, Object> parseBidInfo(String bidInfo) {
        List<Map<String, Object>> purchaser = new ArrayList<>();
        List<Map<String, Object>> winner = new ArrayList<>();
        List<Map<String, Object>> winnerAmount = new ArrayList<>();
        // parse
        List<Object> parsedObjects = ObjUtil.defaultIfNull(JsonUtils.parseJsonArr(bidInfo), new ArrayList<>());
        for (Object parsedObject : parsedObjects) {
            Map<String, Object> parsedMap = (Map<String, Object>) parsedObject;
            // action
            List<Map<String, Object>> actionMaps = (List<Map<String, Object>>) (parsedMap.getOrDefault("action", new ArrayList<>()));
            for (Map<String, Object> actionMap : actionMaps) {
                // purchaser from client
                List<Map<String, Object>> purchaserMaps = (List<Map<String, Object>>) (actionMap.getOrDefault("client", new ArrayList<>()));
                for (Map<String, Object> purchaserMap : purchaserMaps) {
                    String name = String.valueOf(purchaserMap.getOrDefault("name", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        purchaser.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                        }});
                    }
                }
                // winner from supplier
                Map<String, Object> winnerMap = (Map<String, Object>) (actionMap.getOrDefault("supplier", new HashMap<>()));
                List<Map<String, Object>> winnerEntities = (List<Map<String, Object>>) (winnerMap.getOrDefault("entity", new ArrayList<>()));
                for (Map<String, Object> winnerEntity : winnerEntities) {
                    String name = String.valueOf(winnerEntity.getOrDefault("name", ""));
                    String offerPrice = String.valueOf(winnerEntity.getOrDefault("offer_price", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        // winner
                        winner.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                        }});
                        // winner amount
                        if (StrUtil.isNotBlank(offerPrice)) {
                            winnerAmount.add(new HashMap<String, Object>() {{
                                put("amount", offerPrice);
                            }});
                        }
                    }
                }
            }
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("purchaser", "[" + JsonUtils.toString(purchaser) + "]");
        columnMap.put("bid_winner", "[" + JsonUtils.toString(winner) + "]");
        columnMap.put("winning_bid_amt_json_clean", "[" + JsonUtils.toString(winnerAmount) + "]");
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
