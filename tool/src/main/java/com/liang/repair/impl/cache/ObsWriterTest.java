package com.liang.repair.impl.cache;

import com.liang.common.service.connector.storage.obs.ObsWriter;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

import java.math.BigDecimal;
import java.util.HashMap;

public class ObsWriterTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ObsWriter obsWriter = new ObsWriter("obs://hadoop-obs/flink/test/", ObsWriter.FileFormat.JSON);
        HashMap<String, Object> hashMap = new HashMap<String, Object>() {{
            put("id", new BigDecimal("1.999999"));
            put("name", "json");
        }};
        obsWriter.update(JsonUtils.toString(hashMap));
    }
}
