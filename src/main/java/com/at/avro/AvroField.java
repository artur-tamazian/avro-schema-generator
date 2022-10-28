package com.at.avro;

import com.at.avro.config.AvroConfig;
import org.apache.commons.lang3.StringUtils;
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
    private String doc;

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
            defaultValue = column.getDefaultValue().contains("NULL") ? null : column.getDefaultValue();
        }

        if (avroConfig.isUseSqlCommentsAsDoc()) {
            doc = column.getRemarks();
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

    public String getDoc() {
        return doc;
    }

    public boolean isDocSet() {
        return StringUtils.isNotBlank(doc);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", AvroField.class.getSimpleName() + "[", "]")
            .add("name='" + name + "'")
            .add("type=" + type);

        if (defaultValue != NOT_SET) {
            joiner.add("defaultValue=" + defaultValue);
        }
        if (isDocSet()) {
            joiner.add("doc='" + doc + "'");
        }
        return joiner.toString();
    }
}
