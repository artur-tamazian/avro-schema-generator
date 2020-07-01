package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.AvroField;
import com.at.avro.AvroSchema;

/**
 * Writes avro schema json based on AvroSchema bean.
 *
 * @author artur@callfire.com
 */
public class SchemaFormatter implements Formatter<AvroSchema> {

    @Override
    public String toJson(AvroSchema avroSchema, FormatterConfig config) {
        StringBuilder builder = new StringBuilder();

        builder
                .append("{").append(config.prettyPrintSchema()? "" : " ").append(config.lineSeparator())
                .append(formatLine(config, "type", "record"))
                .append(formatLine(config, "name", avroSchema.getName()))
                .append(formatLine(config, "namespace", avroSchema.getNamespace()));
        
        if (avroSchema.isDocSet()) {
            builder.append(formatLine(config, "doc", avroSchema.getDoc()));
        }

        avroSchema.getCustomProperties().forEach((key, value) -> {
            builder.append(formatLine(config, key, value));
        });

        builder.append(config.lineSeparator())
               .append(config.indent()).append("\"fields\"").append(config.colon()).append("[").append(config.lineSeparator())
               .append(formatFields(config, avroSchema)).append(config.lineSeparator())
               .append(config.indent()).append("]").append(config.lineSeparator())
               .append("}");

        return builder.toString();
    }

    public String formatLine(FormatterConfig config, String name, String value) {
        return config.indent() + "\"" + name + "\"" + config.colon() + "\"" + value + "\"," + (config.prettyPrintSchema()? "" : " ") + config.lineSeparator();
    }

    public String formatFields(FormatterConfig config, AvroSchema schema) {
        StringBuilder fieldsJson = new StringBuilder();
        for (AvroField field : schema.getFields()) {
            Formatter<AvroField> formatter = config.getFormatter(field);
            String json = formatter.toJson(field, config);
            fieldsJson.append(config.indent()).append(config.indent()).append(json).append(",").append(config.lineSeparator());
        }
        fieldsJson.setLength(fieldsJson.length()-2);
        return fieldsJson.toString();
    }
}
