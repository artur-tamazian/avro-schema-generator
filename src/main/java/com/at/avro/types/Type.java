package com.at.avro.types;

/**
 * @author artur@callfire.com
 */
public abstract class Type {

    protected final String primitiveType;

    public Type(String primitiveType) {
        this.primitiveType = primitiveType;
    }

    public String getPrimitiveType() {
        return primitiveType;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + primitiveType + ")";
    }
}
