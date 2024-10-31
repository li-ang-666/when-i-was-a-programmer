package com.liang.flink.basic.repair;

import cn.hutool.core.util.ObjUtil;
import com.liang.common.dto.config.RepairTask;
import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RepairGenerator {
    public List<RepairTask> generate() {
        List<RepairTask> repairTasks = new ArrayList<>();
        // 遍历每个task
        for (RepairTask repairTask : ConfigUtils.getConfig().getRepairTasks()) {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(repairTask.getSourceName());
            List<String> tableNames = jdbcTemplate.queryForList("SHOW TABLES",
                    rs -> rs.getString(1));
            // 遍历所有可能满足的表名
            for (String tableName : tableNames) {
                // 表名满足
                if (!tableName.matches(repairTask.getTableName())) {
                    continue;
                }
                RepairTask newTask = ObjUtil.cloneByStream(repairTask);
                newTask.setTableName(tableName);
                repairTasks.add(newTask);
            }
        }
        return repairTasks;
    }
}
