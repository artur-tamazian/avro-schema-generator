package com.at.avro;

import com.at.avro.config.AvroConfig;
import schemacrawler.schema.Column;

import java.util.StringJoiner;

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
            defaultValue = column.getDefaultValue().contains("NULL") ? null : defaultValue;
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

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", AvroField.class.getSimpleName() + "[", "]")
            .add("name='" + name + "'")
            .add("type=" + type);

        if (defaultValue != NOT_SET) {
            joiner.add("defaultValue=" + defaultValue);
        }
        return joiner.toString();
    }
}
