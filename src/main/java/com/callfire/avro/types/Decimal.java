package com.callfire.avro.types;

import com.callfire.avro.config.AvroConfig;
import schemacrawler.schema.Column;

/**
 * @author artur@callfire.com
 */
public class Decimal extends Type {

    private final int precision;
    private final int scale;
    private final String javaClass;
    private final String logicalType = "decimal";

    public Decimal(Column column, AvroConfig config) {
        super("string");
        this.precision = column.getSize();
        this.scale = column.getDecimalDigits();
        this.javaClass = config.getDecimalTypeClass().getCanonicalName();
    }

    public String getJavaClass() {
        return javaClass;
    }

    public String getLogicalType() {
        return logicalType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
