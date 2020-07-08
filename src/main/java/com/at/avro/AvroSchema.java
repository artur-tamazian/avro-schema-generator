package com.at.avro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import com.at.avro.config.AvroConfig;

import org.apache.commons.lang3.StringUtils;
import schemacrawler.schema.Column;
import schemacrawler.schema.Table;

/**
 * @author artur@callfire.com
 */
public class AvroSchema {
    private final String name;
    private final String namespace;
    private final String doc;

    private List<AvroField> fields;

    private Map<String, String> customProperties = new LinkedHashMap<>();

    public AvroSchema(Table table, AvroConfig avroConfig) {
        this.name = avroConfig.getSchemaNameMapper().apply(table.getName());
        this.namespace = avroConfig.getNamespace();
        this.doc = avroConfig.isUseSqlCommentsAsDoc() ? table.getRemarks() : null;
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
    
    public String getDoc() {
        return doc;
    }
    
    public boolean isDocSet() {
        return StringUtils.isNotBlank(doc);
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
        StringJoiner joiner = new StringJoiner(", ", AvroSchema.class.getSimpleName() + "[", "]")
            .add("name='" + name + "'")
            .add("namespace='" + namespace + "'");
        
        if (isDocSet()) {
            joiner.add("doc='" + doc + "'");
        }
        joiner
            .add("fields=" + fields)
            .add("customProperties=" + customProperties);
        
        return joiner.toString();
    }
}
