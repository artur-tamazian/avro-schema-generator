package com.at.avro.mappers;

import org.modeshape.common.text.Inflector;

import java.util.function.Function;

/**
 * @author artur@callfire.com
 */
public class RemovePlural implements Function<String, String> {

    private final Inflector inflector = new Inflector();

    @Override
    public String apply(String name) {
        return inflector.singularize(name);
    }
}
