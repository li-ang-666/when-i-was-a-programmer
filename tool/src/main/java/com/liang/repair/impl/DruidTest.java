package com.liang.repair.impl;

import com.liang.common.service.connector.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

public class DruidTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("427.test");
        jdbcTemplate.update("update testttt set id = 1", "update jhiji set id = 1");
    }
}
