package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;

/**
 * @author artur@callfire.com
 */
public interface Formatter<T> {

    String toJson(T t, FormatterConfig formatterConfig);
}
