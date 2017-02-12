package com.callfire.avro;

import com.callfire.avro.config.AvroConfig;
import schemacrawler.schema.Column;

/**
 * @author artur@callfire.com
 */
public class AvroField {

    private static final Object NOT_SET = new Object();

    private String name;
    private AvroType type;
    private Object defaultValue = NOT_SET;

    public AvroField(Column column, AvroConfig avroConfig) {
        String columnName = column.getName()
                .replaceAll("`", "")
                .replaceAll("\"", "")
                .replaceAll("'", "");

        name = avroConfig.getFieldNameMapper().apply(columnName);
        type = AvroTypeUtil.getAvroType(column, avroConfig);

        if (avroConfig.isAllFieldsDefaultNull()) {
            defaultValue = null;
        } else if (column.getDefaultValue() != null) {
            defaultValue = column.getDefaultValue();
        }
    }

    public String getName() {
        return name;
    }

    public AvroType getType() {
        return type;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean isDefaultValueSet() {
        return defaultValue != NOT_SET;
    }
}
