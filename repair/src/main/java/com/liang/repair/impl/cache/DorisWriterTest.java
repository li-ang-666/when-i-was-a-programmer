package com.liang.repair.impl.cache;

import cn.hutool.core.util.SerializeUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.repair.service.ConfigHolder;

import java.util.Arrays;

public class DorisWriterTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        DorisWriter dorisWriter = new DorisWriter("dorisSink", 1024 * 1024);
        DorisSchema uniqueSchema = DorisSchema.builder()
                .database("test")
                .tableName("unique_test")
                .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                .derivedColumns(Arrays.asList("id = id + 10", "name = concat(name, '(modified by derived column setting)')"))
                .build();
        DorisOneRow unique = new DorisOneRow(uniqueSchema)
                .put("id", 0)
                .put("name", "UNIQUE")
                .put(DorisSchema.DEFAULT_UNIQUE_DELETE_COLUMN, 0);
        for (int i = 1; i <= 1024 * 1024 * 1024; i++) {
            DorisOneRow clone = SerializeUtil.clone(unique);
            clone.put("id", i);
            dorisWriter.write(clone);
        }
        dorisWriter.flush();
    }
}