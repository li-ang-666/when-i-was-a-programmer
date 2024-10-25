package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import com.vladsch.flexmark.html2md.converter.FlexmarkHtmlConverter;
import com.vladsch.flexmark.util.data.MutableDataSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        String content = new JdbcTemplate("104.data_bid").queryForObject("select content from company_bid where id = 57197", rs -> rs.getString(1));
        System.out.println(htmlToMd(content));
    }

    private static String htmlToMd(String html) {
        MutableDataSet options = new MutableDataSet();
        FlexmarkHtmlConverter converter = FlexmarkHtmlConverter.builder(options).build();
        return converter.convert(html);
    }
}
