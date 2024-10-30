package com.liang.flink.project.group.bfs;

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
    private final JdbcTemplate graphData430 = new JdbcTemplate("430.graph_data");

    public boolean queryHasShareholder(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("company_equity_relation_details")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .toString();
        return graphData430.queryForObject(sql, rs -> rs.getString(1)) != null;
    }

    public void cacheInvested(Collection<String> shareholderIds, Map<String, List<Tuple2<GroupBfsService.Edge, GroupBfsService.Node>>> cachedInvestInfo) {
        cachedInvestInfo.clear();
        String sql = new SQL()
                .SELECT("shareholder_id", "equity_ratio", "company_id")
                .FROM("company_equity_relation_details")
                .WHERE("shareholder_id in " + SqlUtils.formatValue(shareholderIds))
                .toString();
        graphData430.queryForList(sql, rs -> {
            String shareholderId = rs.getString(1);
            String equityRatio = rs.getString(2);
            String companyId = rs.getString(3);
            GroupBfsService.Edge edge = new GroupBfsService.Edge(new BigDecimal(equityRatio));
            GroupBfsService.Node node = new GroupBfsService.Node(companyId);
            cachedInvestInfo.compute(shareholderId, (k, v) -> {
                List<Tuple2<GroupBfsService.Edge, GroupBfsService.Node>> investInfo = ObjUtil.defaultIfNull(v, new ArrayList<>());
                investInfo.add(Tuple2.of(edge, node));
                return investInfo;
            });
            return null;
        });
    }
}
