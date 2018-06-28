package com.at.avro;

import com.at.avro.types.Type;

/**
 * Represents a field type in avro schema.
 *
 * @author artur@callfire.com
 */
public class AvroType {

    private final Type type;
    private final boolean nullable;

    public AvroType(Type type, boolean nullable) {
        this.type = type;
        this.nullable = nullable;
    }

    public Type getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }
}
