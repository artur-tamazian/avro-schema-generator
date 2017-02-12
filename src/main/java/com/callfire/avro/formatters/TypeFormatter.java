package com.callfire.avro.formatters;

import com.callfire.avro.AvroType;
import com.callfire.avro.config.FormatterConfig;
import com.callfire.avro.types.Type;

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
