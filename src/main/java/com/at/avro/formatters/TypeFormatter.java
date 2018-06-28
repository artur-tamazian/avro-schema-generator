package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.types.Type;
import com.at.avro.AvroType;

/**
 * Writes avro field type info.
 *
 * @author artur@callfire.com
 */
public class TypeFormatter implements Formatter<AvroType> {

    @Override
    public String toJson(AvroType avroType, FormatterConfig formatterConfig) {
        Formatter<Type> formatter = formatterConfig.getFormatter(avroType.getType());
        String json = formatter.toJson(avroType.getType(), formatterConfig);

        if (avroType.isNullable()) {
            return "[ \"null\", " + json + " ]";
        }

        return json;
    }
}
