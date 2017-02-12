package com.callfire.avro.formatters;

import com.callfire.avro.config.FormatterConfig;

/**
 * @author artur@callfire.com
 */
public interface Formatter<T> {

    String toJson(T t, FormatterConfig formatterConfig);
}
