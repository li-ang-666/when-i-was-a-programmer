package com.liang.common.service.connector.storage.parquet.schema;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;

import java.io.Serializable;

@Setter
@Getter
@Accessors(chain = true)
public abstract class ReadableSchema implements Serializable {
    protected static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    private String name;

    public static ReadableSchema of(String columnName, String mysqlType) {
        // 整型
        if (mysqlType.contains("int")) {
            if (mysqlType.contains("bigint") && mysqlType.contains("unsigned")) {
                return new DecimalSchema(20, 0).setName(columnName);
            } else {
                return new LongSchema().setName(columnName);
            }
        }
        // 精确浮点
        else if (mysqlType.contains("decimal")) {
            int precision = Integer.parseInt(mysqlType.replaceAll(".*?(\\d+).*?(\\d+).*", "$1"));
            int scale = Integer.parseInt(mysqlType.replaceAll(".*?(\\d+).*?(\\d+).*", "$2"));
            return new DecimalSchema(precision, scale).setName(columnName);
        }
        // 其他
        else {
            return new StringSchema().setName(columnName);
        }
    }

    protected Schema nullableSchema(Schema schema) {
        return Schema.createUnion(NULL_SCHEMA, schema);
    }

    public abstract String getSqlType();

    public abstract Schema getSchema();

    public abstract Object formatValue(Object value);
}
