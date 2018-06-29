package com.at.avro;

import com.at.avro.config.FormatterConfig;
import com.at.avro.formatters.Formatter;

/**
 * Generates avro schema json based on AvroSchema model objects.
 *
 * @author artur@callfire.com
 */
public class SchemaGenerator {

    /** Generates an avro schema based on default formatting configuration. */
    public static String generate(AvroSchema schema) {
        FormatterConfig defaultConfig = FormatterConfig.builder().build();
        return generate(schema, defaultConfig);
    }

    /** Generates an avro schema based on a given FormatterConfig */
    public static String generate(AvroSchema schema, FormatterConfig config) {
        Formatter<AvroSchema> formatter = config.getFormatter(schema);
        return formatter.toJson(schema, config);
    }
}
