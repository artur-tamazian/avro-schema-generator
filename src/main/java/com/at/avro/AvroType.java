package com.at.avro;

import java.util.StringJoiner;

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

    @Override
    public String toString() {
        return new StringJoiner(", ", AvroType.class.getSimpleName() + "[", "]")
                .add("type=" + type)
                .add("nullable=" + nullable)
                .toString();
    }
}
