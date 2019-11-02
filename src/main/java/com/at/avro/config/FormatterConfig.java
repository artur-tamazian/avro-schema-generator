package com.at.avro.config;

import java.util.HashMap;
import java.util.Map;

import com.at.avro.AvroField;
import com.at.avro.AvroSchema;
import com.at.avro.AvroType;
import com.at.avro.formatters.*;
import com.at.avro.types.*;
import com.at.avro.types.Enum;

/**
 * Avro schema json formatters configuration.
 * It has default formatters for each Avro bean (ie AvroSchema, AvroField, etc).
 * They can be customized by implementing Formatter interface and registering it as FormatterConfig::setFormatter.
 *
 * @author artur@callfire.com
 */
public class FormatterConfig {

    private String indent;
    private String lineSeparator;
    private String colon;
    private boolean prettyPrintFields;
    private boolean prettyPrintSchema;

    private Map<Class, Formatter> formatters = new HashMap<Class, Formatter>() {{
            put(AvroSchema.class, new SchemaFormatter());
            put(AvroField.class, new FieldFormatter());
            put(AvroType.class, new TypeFormatter());
            put(Date.class, new DateFormatter());
            put(Enum.class, new EnumFormatter());
            put(Primitive.class, new PrimitiveFormatter());
            put(Decimal.class, new DecimalFormatter());
            put(Array.class, new ArrayFormatter());
    }};

    private FormatterConfig() {
    }

    public String indent() {
        return indent;
    }

    public String indent(int times) {
        String result = "";
        for (int i = 0; i < times; i++) {
            result += indent();
        }
        return result;
    }

    public String colon() {
        return colon;
    }

    public String lineSeparator() {
        return lineSeparator;
    }

    public boolean prettyPrintFields() {
        return prettyPrintFields;
    }

    public boolean prettyPrintSchema() {
        return prettyPrintSchema;
    }

    public <T> Formatter<T> getFormatter(T dto) {
        if (!formatters.containsKey(dto.getClass())) {
            throw new IllegalArgumentException("Formatter not found for " + dto.getClass().getSimpleName());
        }
        return formatters.get(dto.getClass());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<Class, Formatter> formatters = new HashMap<>();
        private boolean prettyPrintSchema = true;
        private boolean prettyPrintFields = false;
        private boolean addSpaceAfterColon = true;
        private String indent = "  ";

        public <T> Builder setFormatter(Class<T> dtoClass, Formatter<T> formatter) {
            formatters.put(dtoClass, formatter);
            return this;
        }

        /** False - to print schema in one line, true - to print it nicely formatted. */
        public Builder setPrettyPrintSchema(boolean prettyPrintSchema) {
            this.prettyPrintSchema = prettyPrintSchema;
            return this;
        }

        /**
         * When false - each field will be written as one line, even if pretty printing of schema is set to true.
         * if prettyPrintSchema is false, prettyPrintFields is ignored.
         */
        public Builder setPrettyPrintFields(boolean prettyPrintFields) {
            this.prettyPrintFields = prettyPrintFields;
            return this;
        }

        /** Set indent value for pretty printing. */
        public Builder setIndent(String indent) {
            this.indent = indent;
            return this;
        }

        public FormatterConfig build() {
            FormatterConfig config = new FormatterConfig();
            config.formatters.putAll(this.formatters);
            config.lineSeparator = prettyPrintSchema ? "\n" : "";
            config.colon = addSpaceAfterColon ? ": " : ":";
            config.prettyPrintFields = prettyPrintFields && prettyPrintSchema;
            config.prettyPrintSchema = prettyPrintSchema;
            config.indent = prettyPrintSchema ? indent : "";
            return config;
        }
    }
}
