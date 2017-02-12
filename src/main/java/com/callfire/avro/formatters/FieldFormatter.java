package com.callfire.avro.formatters;

import com.callfire.avro.AvroField;
import com.callfire.avro.AvroType;
import com.callfire.avro.config.FormatterConfig;

/**
 * @author artur@callfire.com
 */
public class FieldFormatter implements Formatter<AvroField> {

    @Override
    public String toJson(AvroField field, FormatterConfig config) {
        StringBuilder builder = new StringBuilder();

        String fieldLineSeparator = config.prettyPrintFields() ? "\n" : " ";
        Formatter<AvroType> formatter = config.getFormatter(field.getType());
        String typeJson = formatter.toJson(field.getType(), config);
        String valueIndent = config.prettyPrintFields() ? config.indent(3) : "";

        builder = builder
                .append("{")
                .append(fieldLineSeparator).append(valueIndent)
                .append("\"name\"").append(config.colon()).append("\"").append(field.getName()).append("\",")
                .append(fieldLineSeparator).append(valueIndent)
                .append("\"type\"").append(config.colon()).append(typeJson).append(",");

        if (field.isDefaultValueSet()) {
            String defaultValue = shouldDefaultBeQuoted(field) ? "\"" + field.getDefaultValue() + "\"" : field.getDefaultValue() + "";
            builder = builder.append(fieldLineSeparator).append(valueIndent)
                    .append("\"default\"").append(config.colon()).append(defaultValue);
        }
        else {
            builder.setLength(builder.length() - 1);
        }

        builder.append(fieldLineSeparator).append(config.prettyPrintFields() ? config.indent(2) : "").append("}");

        return builder.toString();
    }

    private boolean shouldDefaultBeQuoted(AvroField avroField) {
        return avroField.getType().getType().getPrimitiveType().equals("string") && avroField.getDefaultValue() != null;
    }
}
