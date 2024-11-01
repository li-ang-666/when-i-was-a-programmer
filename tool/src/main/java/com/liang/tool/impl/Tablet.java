package com.liang.tool.impl;

import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.tool.service.ConfigHolder;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Tablet extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate doris = new JdbcTemplate("doris");
        List<String> dbIds = doris.queryForList("SHOW PROC '/cluster_health/tablet_health'", rs -> rs.getString("DbId"));
        for (String dbId : dbIds) {
            if (StringUtils.isNumeric(dbId)) {
                List<Map<String, Object>> columnMaps = doris.queryForColumnMaps(String.format("SHOW PROC '/cluster_health/tablet_health/%s'", dbId));
                Map<String, Object> columnMap = columnMaps.get(0);
                List<String> brokenTablets = columnMap.values().stream()
                        .filter(e -> StringUtils.isNotBlank(String.valueOf(e)))
                        .flatMap(e -> Arrays.stream(String.valueOf(e).split(",")))
                        .collect(Collectors.toList());
                for (String tablet : brokenTablets) {
                    String detailCmd = doris.queryForObject("show tablet " + tablet, rs -> rs.getString("DetailCmd"));
                    columnMaps = doris.queryForColumnMaps(detailCmd.replaceAll(";", ""));
                    columnMaps.stream()
                            .filter(e -> !"-1".equals(e.get("LstFailedVersion")) || e.get("LstFailedTime") != null)
                            .forEach(e -> {
                                String cmd = String.format("ADMIN SET REPLICA STATUS PROPERTIES(\"tablet_id\" = \"%s\", \"backend_id\" = \"%s\", \"status\" = \"bad\")", tablet, e.get("BackendId"));
                                doris.update(cmd);
                            });
                }
            }
        }
    }
}
