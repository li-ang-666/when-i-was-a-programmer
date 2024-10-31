package com.liang.flink.project.dim.count.dao;

import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ApolloUtils;
import com.liang.common.util.SqlUtils;

public class EntityBeneficiaryDetailsDao {
    private final JdbcTemplate jdbcTemplate = new JdbcTemplate("bdpEquity");

    public String queryCount(String shareholderId) {
        String sql = ApolloUtils.get("EntityBeneficiaryDetailsCount");
        sql = String.format(sql, SqlUtils.formatValue(shareholderId));
        return jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
    }
}
