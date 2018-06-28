package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.types.Primitive;

/**
 * @author artur@callfire.com
 */
public class PrimitiveFormatter implements Formatter<Primitive> {
    @Override
    public String toJson(Primitive primitive, FormatterConfig config) {
        return "\"" + primitive.getPrimitiveType() + "\"";
    }
}
