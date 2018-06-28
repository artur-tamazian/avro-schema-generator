package com.at.avro.mappers;

import org.modeshape.common.text.Inflector;

import java.util.function.Function;

/**
 * @author artur@callfire.com
 */
public class ToCamelCase implements Function<String, String> {

    private final Inflector inflector = new Inflector();

    @Override
    public String apply(String name) {
        while (name.startsWith("_")) {
            name = name.substring(1);
        }
        return inflector.upperCamelCase(name, '_');
    }
}
