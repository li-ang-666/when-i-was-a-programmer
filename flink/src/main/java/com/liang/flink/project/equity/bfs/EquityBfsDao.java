package com.liang.flink.project.equity.bfs;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.flink.project.equity.bfs.dto.ShareholderJudgeInfo;
import com.liang.flink.project.equity.bfs.dto.mysql.CompanyEquityRelationDetailsDto;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class EquityBfsDao {
    private static final List<String> NOT_ALIVE_TAG_ID_LIST = Arrays.asList("34", "35", "36", "37", "38", "39", "40", "43", "44", "46", "47", "48", "49", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "67", "68");
    private final JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate companyBase142 = new JdbcTemplate("142.company_base");
    private final JdbcTemplate companyBase465 = new JdbcTemplate("465.company_base");
    private final JdbcTemplate humanBase040 = new JdbcTemplate("040.human_base");
    private final JdbcTemplate prism116 = new JdbcTemplate("116.prism");

    public Map<String, Object> queryCompanyInfo(String companyId) {
        String sql = new SQL()
                .SELECT("company_id", "company_name", "unified_social_credit_code", "org_type")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        List<Map<String, Object>> columnMaps = companyBase435.queryForColumnMaps(sql);
        if (columnMaps.isEmpty()) {
            return new HashMap<>();
        }
        return columnMaps.get(0);
    }

    public boolean isListed(Object companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_equity_relation_details")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("data_source in (100, -100)")
                .toString();
        return graphData430.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public String queryEntityProperty(String companyId) {
        String sql = new SQL()
                .SELECT("entity_property")
                .FROM("tyc_entity_general_property_reference")
                .WHERE("tyc_unique_entity_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase465.queryForObject(sql, rs -> rs.getString(1));
    }

    public Map<String, List<CompanyEquityRelationDetailsDto>> queryThisLevelShareholder(Set<String> investedCompanyIds) {
        List<List<String>> splits = split(investedCompanyIds);
        log.debug("split to {} times query", splits.size());
        Map<String, List<CompanyEquityRelationDetailsDto>> res = new HashMap<>();
        for (List<String> split : splits) {
            String sql = new SQL()
                    .SELECT("company_id_invested")
                    .SELECT("tyc_unique_entity_id_investor")
                    .SELECT("tyc_unique_entity_name_investor")
                    .SELECT("equity_ratio")
                    .FROM("company_equity_relation_details")
                    .WHERE("company_id_invested in " + SqlUtils.formatValue(split))
                    .WHERE("reference_pt_year = 2024")
                    .toString();
            graphData430.queryForList(sql, rs -> {
                String investedCompanyId = rs.getString(1);
                String id = rs.getString(2);
                String name = rs.getString(3);
                BigDecimal ratio = new BigDecimal(StrUtil.nullToDefault(rs.getString(4), "0"));
                res.compute(investedCompanyId, (k, v) -> {
                    List<CompanyEquityRelationDetailsDto> shareholders = (v != null) ? v : new ArrayList<>();
                    shareholders.add(new CompanyEquityRelationDetailsDto(id, name, ratio));
                    return shareholders;
                });
                return null;
            });
        }
        return res;
    }

    public Map<String, ShareholderJudgeInfo> queryShareholderJudgeInfo(Set<String> companyIds) {
        List<List<String>> splits = split(companyIds);
        log.debug("split to {} times query", splits.size());
        Map<String, ShareholderJudgeInfo> res = new HashMap<>();
        for (List<String> split : splits) {
            String t1 = split.parallelStream().map(companyId -> String.format("select %s as company_id", SqlUtils.formatValue(companyId)))
                    .collect(Collectors.joining(" union all "));
            String t2 = new SQL().SELECT("company_id", "max(1) as is_closed")
                    .FROM("bdp_company_profile_tag_details_total")
                    .WHERE("company_id in " + SqlUtils.formatValue(split))
                    .WHERE("profile_tag_id in " + SqlUtils.formatValue(NOT_ALIVE_TAG_ID_LIST))
                    .WHERE("deleted = 0")
                    .GROUP_BY("company_id")
                    .toString();
            String t3 = new SQL()
                    .SELECT("company_id", "max(1) as is_001")
                    .FROM("company_001_company_list_total")
                    .WHERE("company_id in " + SqlUtils.formatValue(split))
                    .WHERE("deleted = 0")
                    .GROUP_BY("company_id")
                    .toString();
            String sql = new SQL()
                    .SELECT("t1.company_id")
                    .SELECT("ifnull(t2.is_closed, false)")
                    .SELECT("ifnull(t3.is_001, false)")
                    .FROM(String.format("(%s)t1", t1))
                    .LEFT_OUTER_JOIN(String.format("(%s)t2 on t1.company_id = t2.company_id", t2))
                    .LEFT_OUTER_JOIN(String.format("(%s)t3 on t1.company_id = t3.company_id", t3))
                    .toString();
            Map<String, ShareholderJudgeInfo> splitRes = companyBase142.queryForList(sql, rs -> {
                String id = rs.getString(1);
                boolean isClosed = rs.getBoolean(2);
                boolean is001 = rs.getBoolean(3);
                return new ShareholderJudgeInfo(id, isClosed, is001);
            }).parallelStream().collect(Collectors.toMap(ShareholderJudgeInfo::getCompanyId, e -> e));
            res.putAll(splitRes);
        }
        return res;
    }

    /**
     * human or company
     */
    public Map<String, Object> queryHumanOrCompanyInfo(String id) {
        String sql = id.length() == 17 ?
                new SQL()
                        .SELECT("human_name_id", "master_company_id", "human_name")
                        .FROM("human")
                        .WHERE("human_id = " + SqlUtils.formatValue(id))
                        .toString() :
                new SQL()
                        .SELECT("company_id", "company_id", "company_name")
                        .FROM("company_index")
                        .WHERE("company_id = " + SqlUtils.formatValue(id))
                        .toString();
        JdbcTemplate jdbcTemplate = id.length() == 17 ?
                humanBase040 :
                companyBase435;
        Map<String, Object> columnMap = jdbcTemplate.queryForObject(sql, rs -> new HashMap<String, Object>() {{
            put("name_id", rs.getString(1));
            put("company_id", rs.getString(2));
            put("name", rs.getString(3));
            put("id", id);
        }});
        return columnMap != null ? columnMap : new HashMap<>();
    }

    public Map<String, Map<String, Object>> batchQueryHumanOrCompanyInfo(Set<String> ids) {
        String sampleId = CollUtil.get(ids, 0);
        List<List<String>> splits = split(ids);
        log.debug("split to {} times query", splits.size());
        Map<String, Map<String, Object>> res = new HashMap<>();
        for (List<String> split : splits) {
            String sql = sampleId.length() == 17 ?
                    new SQL()
                            .SELECT("human_name_id", "master_company_id", "human_name", "human_id")
                            .FROM("human")
                            .WHERE("human_id in " + SqlUtils.formatValue(split))
                            .toString() :
                    new SQL()
                            .SELECT("company_id", "company_id", "company_name", "company_id")
                            .FROM("company_index")
                            .WHERE("company_id in " + SqlUtils.formatValue(split))
                            .toString();
            JdbcTemplate jdbcTemplate = sampleId.length() == 17 ?
                    humanBase040 :
                    companyBase435;
            Map<String, HashMap<String, Object>> splitRes = jdbcTemplate.queryForList(sql, rs -> new HashMap<String, Object>() {{
                put("name_id", rs.getString(1));
                put("company_id", rs.getString(2));
                put("name", rs.getString(3));
                put("id", rs.getString(4));
            }}).parallelStream().collect(Collectors.toMap(e -> String.valueOf(e.get("id")), e -> e));
            res.putAll(splitRes);
        }
        return res;
    }

    private <T> List<List<T>> split(Collection<T> collection) {
        return CollUtil.split(collection, 512);
    }
}
