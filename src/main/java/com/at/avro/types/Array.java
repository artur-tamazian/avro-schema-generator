package com.at.avro.types;

public class Array extends Type {
    public Array(Type itemsType) {
        super(itemsType.getPrimitiveType());
    }
}
