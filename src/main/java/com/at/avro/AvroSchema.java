package com.at.avro;

import com.at.avro.config.AvroConfig;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;

import java.util.*;

/**
 * @author artur@callfire.com
 */
public class AvroSchema {
    private final String name;
    private final String namespace;

    private List<AvroField> fields;

    private Map<String, String> customProperties = new LinkedHashMap<>();

    public AvroSchema(Table table, AvroConfig avroConfig) {
        this.name = avroConfig.getSchemaNameMapper().apply(table.getName());
        this.namespace = avroConfig.getNamespace();
        this.fields = new ArrayList<>(table.getColumns().size());

        for (Column column : table.getColumns()) {
            this.fields.add(new AvroField(column, avroConfig));
        }

        avroConfig.getAvroSchemaPostProcessor().accept(this, table);
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public List<AvroField> getFields() {
        return fields;
    }

    public void addCustomProperty(String name, String value) {
        customProperties.put(name, value);
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }
}
