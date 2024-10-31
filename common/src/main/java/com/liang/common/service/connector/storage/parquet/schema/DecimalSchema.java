package com.liang.common.service.connector.storage.parquet.schema;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.PackagePrivate;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Getter
@Setter
@AllArgsConstructor
@PackagePrivate
class DecimalSchema extends ReadableSchema {
    private int precision;
    private int scale;

    @Override
    public String getSqlType() {
        if (precision > 38) {
            return "STRING";
        }
        return String.format("DECIMAL(%d, %d)", precision, scale);
    }

    @Override
    public Schema getSchema() {
        if (precision > 38) {
            return nullableSchema(Schema.create(Schema.Type.STRING));
        }
        Schema schema = (precision > 18)
                ? Schema.create(Schema.Type.BYTES)
                : Schema.createFixed(getName(), null, null, computeMinBytesForDecimalPrecision(precision));
        return nullableSchema(LogicalTypes.decimal(precision, scale).addToSchema(schema));
    }

    @Override
    public Object formatValue(Object value) {
        if (value == null) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(value.toString()).setScale(scale, RoundingMode.DOWN);
        if (precision > 38) {
            return bigDecimal.toPlainString();
        }
        return bigDecimal;
    }

    private int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }
}
