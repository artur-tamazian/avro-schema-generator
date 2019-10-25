package com.at.avro.types;

import java.util.Arrays;

import schemacrawler.schema.Column;

/**
 * @author artur@callfire.com
 */
public class Enum extends Type {

    private final String name;
    private final String[] symbols;

    public Enum(Column column) {
        super("enum");
        this.name = column.getName();

        String allowedValues = column.getAttribute("COLUMN_TYPE").toString();
        this.symbols = allowedValues
                .replaceFirst("enum", "")
                .replaceFirst("ENUM", "")
                .replace(")", "")
                .replace("(", "")
                .split(",");

        for (int i = 0; i < symbols.length; i++) {
            symbols[i] = symbols[i].trim().replaceAll("'", "");
        }
    }

    public String getName() {
        return name;
    }

    public String[] getSymbols() {
        return symbols;
    }

    @Override
    public String toString() {
        return super.toString() + ": " + getName() + "[" + Arrays.toString(getSymbols()) + "]";
    }
}
