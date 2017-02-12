package com.callfire.avro.formatters;

import com.callfire.avro.config.FormatterConfig;
import com.callfire.avro.types.Primitive;

/**
 * @author artur@callfire.com
 */
public class PrimitiveFormatter implements Formatter<Primitive> {
    @Override
    public String toJson(Primitive primitive, FormatterConfig config) {
        return "\"" + primitive.getPrimitiveType() + "\"";
    }
}
