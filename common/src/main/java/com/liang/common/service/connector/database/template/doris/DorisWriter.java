package com.liang.common.service.connector.database.template.doris;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DorisWriter {
    private static final byte JSON_PREFIX = (byte) '[';
    private static final byte JSON_SEPARATOR = (byte) ',';
    private static final byte JSON_SUFFIX = (byte) ']';
    private final DorisHelper dorisHelper;
    private final ByteBuffer buffer;
    // init when first row
    private DorisSchema dorisSchema;
    private List<String> keys;

    public DorisWriter(String name, int bufferSize) {
        buffer = ByteBuffer.allocate(bufferSize);
        dorisHelper = new DorisHelper(ConfigUtils.getConfig().getDorisConfigs().get(name));
    }

    public void write(DorisOneRow dorisOneRow) {
        synchronized (buffer) {
            Map<String, Object> columnMap = dorisOneRow.getColumnMap();
            // the first row
            if (dorisSchema == null) {
                dorisSchema = dorisOneRow.getSchema();
                keys = new ArrayList<>(columnMap.keySet());
            }
            byte[] content = JsonUtils.toBytes(columnMap);
            // 1(separator) + content + 1(suffix)
            if (buffer.position() + (1 + content.length + 1) > buffer.limit()) flush();
            buffer.put(JSON_SEPARATOR);
            buffer.put(content);
        }
    }

    public void flush() {
        synchronized (buffer) {
            if (buffer.position() > 0) {
                buffer.put(0, JSON_PREFIX);
                buffer.put(JSON_SUFFIX);
                dorisHelper.executePut(dorisSchema.getDatabase(), dorisSchema.getTableName(), this::setPut);
            }
            buffer.clear();
        }
    }

    private void setPut(HttpPut put) {
        // format
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("fuzzy_parse", "true");
        put.setHeader("num_as_string", "true");
        // columns
        if (CollUtil.isNotEmpty(dorisSchema.getDerivedColumns())) {
            put.setHeader("columns", parseColumns());
        }
        // unique delete
        if (StrUtil.isNotBlank(dorisSchema.getUniqueDeleteOn())) {
            put.setHeader("merge_type", "MERGE");
            put.setHeader("delete", dorisSchema.getUniqueDeleteOn());
            // delete must contains columns or hidden_columns
            if (!put.containsHeader("columns")) {
                put.setHeader("hidden_columns", DorisSchema.DEFAULT_UNIQUE_DELETE_COLUMN);
            }
        }
        // where
        if (StrUtil.isNotBlank(dorisSchema.getWhere())) {
            put.setHeader("where", dorisSchema.getWhere());
        }
        // entity
        put.setEntity(new ByteArrayEntity(buffer.array(), 0, buffer.position()));
    }

    private String parseColumns() {
        List<String> columns = keys.parallelStream()
                .map(e -> "`" + e + "`")
                .collect(Collectors.toList());
        columns.addAll(dorisSchema.getDerivedColumns());
        return String.join(",", columns);
    }
}
