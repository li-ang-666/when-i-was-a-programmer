package com.liang.flink.project.graph.export;

import com.liang.common.service.SQL;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;

import java.util.Arrays;
import java.util.List;

public class GraphExportDao {
    private static final List<String> NOT_ALIVE_TAG_ID_LIST = Arrays.asList("34", "35", "36", "37", "38", "39", "40", "43", "44", "46", "47", "48", "49", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "67", "68");
    private final JdbcTemplate companyBase142 = new JdbcTemplate("142.company_base");

    public boolean isClosed(String companyId) {
        String sql = new SQL().SELECT("1")
                .FROM("bdp_company_profile_tag_details_total")
                .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                .WHERE("profile_tag_id in " + SqlUtils.formatValue(NOT_ALIVE_TAG_ID_LIST))
                .WHERE("deleted = 0")
                .toString();
        return companyBase142.queryForObject(sql, rs -> rs.getString(1)) != null;
    }
}
