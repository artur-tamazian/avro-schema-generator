package com.callfire.avro.types;

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
                .replace(")", "")
                .replace("(", "")
                .replaceAll("'", "")
                .split(",");
    }

    public String getName() {
        return name;
    }

    public String[] getSymbols() {
        return symbols;
    }
}
