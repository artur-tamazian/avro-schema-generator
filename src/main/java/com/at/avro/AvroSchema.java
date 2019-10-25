package com.at.avro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import com.at.avro.config.AvroConfig;

import schemacrawler.schema.Column;
import schemacrawler.schema.Table;

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

    @Override
    public String toString() {
        return new StringJoiner(", ", AvroSchema.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("namespace='" + namespace + "'")
                .add("fields=" + fields)
                .add("customProperties=" + customProperties)
                .toString();
    }
}
