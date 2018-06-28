package com.at.avro;

import com.at.avro.config.AvroConfig;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.*;
import schemacrawler.utility.SchemaCrawlerUtility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Connects to a db and populates {@link AvroSchema} beans for existing tables.
 *
 * @author artur@callfire.com
 */
public class DbSchemaExtractor {

    private final String connectionUrl;
    private final Properties connectionProperties;

    public DbSchemaExtractor(String connectionUrl, String user, String password) {
        this.connectionProperties = new Properties();
        this.connectionProperties.put("nullNamePatternMatchesAll", "true");
        this.connectionProperties.put("user", user);
        this.connectionProperties.put("password", password);

        this.connectionUrl = connectionUrl;
    }

    /** Returns all {@link AvroSchema}s that are present in a target DB */
    public List<AvroSchema> getAll(AvroConfig avroConfig) {
        return get(avroConfig, null);
    }

    /** Returns all {@link AvroSchema}s that are present in given DB schema */
    public List<AvroSchema> getForSchema(AvroConfig avroConfig, String dbSchemaName) {
        return get(avroConfig, dbSchemaName);
    }

    /** Returns {@link AvroSchema}s for each of given tables */
    public List<AvroSchema> getForTables(AvroConfig avroConfig, String dbSchemaName, String... tableNames) {
        return get(avroConfig, dbSchemaName, tableNames);
    }

    /** Returns {@link AvroSchema} for a specific table */
    public AvroSchema getForTable(AvroConfig avroConfig, String dbSchemaName, String tableName) {
        List<AvroSchema> schemas = get(avroConfig, dbSchemaName, tableName);
        if (schemas.isEmpty()) {
            return null;
        }
        else {
            return schemas.get(0);
        }
    }

    private List<AvroSchema> get(AvroConfig avroConfig, String dbSchemaName, String... tableNames) {
        try (Connection connection = DriverManager.getConnection(connectionUrl, connectionProperties)) {

            SchemaCrawlerOptions crawlerOptions = defaultCrawlerOptions();
            if (dbSchemaName != null) {
                crawlerOptions.setSchemaInclusionRule(new RegularExpressionInclusionRule(".*((?i)" + dbSchemaName + ")"));
            }

            Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, crawlerOptions);

            List<Schema> dbSchemas = new ArrayList<>(catalog.getSchemas());
            if (dbSchemaName != null) {
                dbSchemas = dbSchemas.stream()
                        .filter(schema -> dbSchemaName.equalsIgnoreCase(schema.getCatalogName()) || dbSchemaName.equalsIgnoreCase(schema.getName()))
                        .collect(toList());
            }

            List<AvroSchema> schemas = new LinkedList<>();

            for (Schema dbSchema : dbSchemas) {
                for (Table table : catalog.getTables(dbSchema)) {
                    if (tableNames.length == 0 || containsIgnoreCase(tableNames, table.getName())) {
                        schemas.add(new AvroSchema(table, avroConfig));
                    }
                }
            }

            return schemas;
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Can not get connection to " + connectionUrl, e);
        }
        catch (SchemaCrawlerException e) {
            throw new RuntimeException(e);
        }
    }

    private SchemaCrawlerOptions defaultCrawlerOptions() {
        SchemaCrawlerOptions crawlerOptions = new SchemaCrawlerOptions();
        crawlerOptions.setTableNamePattern("%");
        crawlerOptions.setRoutineInclusionRule(new ExcludeAll());
        crawlerOptions.setSchemaInfoLevel(SchemaInfoLevelBuilder.maximum());
        crawlerOptions.setColumnInclusionRule(new IncludeAll());
        return crawlerOptions;
    }

    private boolean containsIgnoreCase(String[] array, String word) {
        for (String s : array) {
            if (s.equalsIgnoreCase(word)) {
                return true;
            }
        }
        return false;
    }
}
