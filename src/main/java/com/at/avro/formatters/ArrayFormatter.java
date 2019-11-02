package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.types.Array;

import static java.lang.String.format;

public class ArrayFormatter implements com.at.avro.formatters.Formatter<Array> {
    @Override
    public String toJson(Array array, FormatterConfig formatterConfig) {
        return format("{ \"type\": \"array\", \"items\": \"%s\" }", array.getPrimitiveType());
    }
}
