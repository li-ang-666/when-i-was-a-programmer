package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

// has_controller
// num_control_ability
public class ReadHbase extends ConfigHolder {
    private final static HbaseTemplate HBASE_TEMPLATE;

    static {
        HBASE_TEMPLATE = new HbaseTemplate("hbaseSink");
    }

    public static void main(String[] args) {
        HbaseOneRow queryResult = HBASE_TEMPLATE.getRow(new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "22822"));
        log.info("{}", JsonUtils.toString(queryResult));
        //queryResult.put("bid_count", 2);
        HBASE_TEMPLATE.update(queryResult);
    }
}
