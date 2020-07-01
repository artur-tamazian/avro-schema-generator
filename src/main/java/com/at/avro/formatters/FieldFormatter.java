package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.AvroField;
import com.at.avro.AvroType;

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
                    .append("\"default\"").append(config.colon()).append(defaultValue).append(",");
        }
        
        if (field.isDocSet()) {
            builder = builder
                .append(fieldLineSeparator).append(valueIndent)
                .append("\"doc\"").append(config.colon()).append("\"").append(field.getDoc()).append("\"");
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
