package com.liang.common.service.connector.storage.parquet.schema;

import lombok.experimental.PackagePrivate;
import org.apache.avro.Schema;

@PackagePrivate
class StringSchema extends ReadableSchema {
    @Override
    public String getSqlType() {
        return "STRING";
    }

    @Override
    public Schema getSchema() {
        return nullableSchema(Schema.create(Schema.Type.STRING));
    }

    @Override
    public Object formatValue(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }
}
