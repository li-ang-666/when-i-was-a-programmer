package com.liang.repair.impl;

import com.liang.common.dto.config.DBConfig;
import com.liang.common.util.ConfigUtils;
import com.liang.repair.service.ConfigHolder;

import java.util.Map;

public class TestYamlGenerate extends ConfigHolder {
    public static void main(String[] args) {
        Map<String, DBConfig> dbConfigs = ConfigUtils.getConfig().getDbConfigs();
        System.out.println("dbConfigs:");
        dbConfigs.forEach((name, dbConfig) -> {
            dbConfig.setHost("9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com");
            dbConfig.setPort(3306);
            dbConfig.setUser("jdtest_d_data_ddl");
            dbConfig.setPassword("dwjIFAmM39Y2O98cKu");
            String format = String.format("  %s: { host: \"%s\", database: \"%s\", user: \"%s\", password: \"%s\", port: %s }",
                    name, dbConfig.getHost(), dbConfig.getDatabase(), dbConfig.getUser(), dbConfig.getPassword(), dbConfig.getPort());
            System.out.println(format);
        });
    }
}
