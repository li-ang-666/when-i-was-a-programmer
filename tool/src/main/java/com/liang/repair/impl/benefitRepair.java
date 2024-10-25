package com.liang.repair.impl;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.repair.service.ConfigHolder;

public class benefitRepair extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("225.prism_shareholder_path");
        String companyId = "2310367298";
        long index = Long.parseLong(companyId) % 10000 % 64;
        String table = index >= 10 ? "ratio_path_shareholder_0" + index : "ratio_path_shareholder_00" + index;
        String sql = new SQL().UPDATE(table)
                .SET("deleted = 1")
                .WHERE("company_graph_id = " + SqlUtils.formatValue(companyId))
                .toString();
        jdbcTemplate.update(sql);
    }
}
