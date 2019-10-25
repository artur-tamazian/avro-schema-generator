package com.at.avro.types;

import com.at.avro.config.AvroConfig;

import schemacrawler.schema.Column;

/**
 * @author artur@callfire.com
 */
public class Date extends Type {

    private final String logicalType = "timestamp-millis";
    private final String javaClass;

    public Date(Column column, AvroConfig config) {
        super("long");
        this.javaClass = config.getDateTypeClass().getCanonicalName();
    }

    public String getLogicalType() {
        return logicalType;
    }

    public String getJavaClass() {
        return javaClass;
    }

    @Override
    public String toString() {
        return super.toString() + ": " + getLogicalType();
    }
}
