package com.liang.repair.impl;

import cn.hutool.core.io.IoUtil;
import com.liang.repair.service.ConfigHolder;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class EquityBfsCreateTable extends ConfigHolder {
    public static void main(String[] args) {
        InputStream resourceAsStream = EquityBfsCreateTable.class.getClassLoader().getResourceAsStream("equity-bfs-create-table.sql");
        String read = IoUtil.read(resourceAsStream, StandardCharsets.UTF_8);
        for (int i = 0; i < 100; i++) {
            System.out.println(read.replaceAll("ratio_path_company_new", "ratio_path_company_new_" + i));
        }
        for (int i = 0; i < 100; i++) {
            //System.out.println("truncate table ratio_path_company_new_" + i + ";");
        }
    }
}
