package com.liang.tool.impl.cache;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.connector.database.template.HbaseTemplate;
import com.liang.tool.service.ConfigHolder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class HbaseTemplateTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        hbaseTemplate.enableCache(5000, 1024);

        HbaseSchema schema1 = HbaseSchema.builder()
                .namespace("prism_c")
                .tableName("human_all_count")
                .columnFamily("cf")
                .rowKeyReverse(false)
                .build();

        HbaseSchema schema2 = HbaseSchema.builder()
                .namespace("prism_c")
                .tableName("company_all_count")
                .columnFamily("count")
                .rowKeyReverse(true)
                .build();

        HbaseOneRow row1 = new HbaseOneRow(schema1, "abc")
                .put("id", 1)
                .put("name", "tom");

        HbaseOneRow row2 = new HbaseOneRow(schema2, "abc")
                .put("id", 1)
                .put("name", "jerry");

        ArrayList<HbaseOneRow> hbaseOneRows = new ArrayList<>();
        for (int i = 1; i <= 1024; i++) {
            hbaseOneRows.add(row1);
        }

        hbaseTemplate.update(row1, row2);
        hbaseTemplate.update(hbaseOneRows);
        TimeUnit.SECONDS.sleep(10);
    }
}
