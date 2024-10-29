package com.liang.flink.project.group.bfs;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ObjUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
public class GroupBfsDao {
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");

    public Map<String, Object> queryCompanyIndex(String companyId) {
        String sql = new SQL().SELECT("*")
                .FROM("company_index")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return companyBase435.queryForColumnMap(sql);
    }

    public boolean queryHasShareholder(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_equity_relation_details")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return graphData430.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public void cacheInvested(Collection<String> shareholderIds, Map<String, List<Tuple2<GroupBfsService.Edge, GroupBfsService.Node>>> cachedInvestInfo) {
        cachedInvestInfo.clear();
        //shareholderIds.removeIf(cachedInvestInfo::containsKey);
        for (List<String> subShareholders : CollUtil.split(shareholderIds, 1000)) {
            String sql = new SQL()
                    .SELECT("shareholder_id", "equity_ratio", "company_id", "company_name")
                    .FROM("company_equity_relation_details")
                    .WHERE("shareholder_id in " + SqlUtils.formatValue(subShareholders))
                    .toString();
            graphData430.streamQuery(false, sql, rs -> {
                String shareholderId = rs.getString(1);
                String equityRatio = rs.getString(2);
                String companyId = rs.getString(3);
                String companyName = rs.getString(4);
                GroupBfsService.Edge edge = new GroupBfsService.Edge(new BigDecimal(equityRatio));
                GroupBfsService.Node node = new GroupBfsService.Node(companyId, companyName);
                cachedInvestInfo.compute(shareholderId, (k, v) -> {
                    List<Tuple2<GroupBfsService.Edge, GroupBfsService.Node>> investInfo = ObjUtil.defaultIfNull(v, new ArrayList<>());
                    investInfo.add(Tuple2.of(edge, node));
                    return investInfo;
                });
            });
        }
    }
}
