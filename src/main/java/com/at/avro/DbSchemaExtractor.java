package com.at.avro;

import com.at.avro.config.AvroConfig;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.utility.SchemaCrawlerUtility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
import static schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions;
import static schemacrawler.schemacrawler.SchemaInfoLevelBuilder.maximum;

/**
 * Connects to a db and populates AvroSchema beans for existing tables.
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

    /** Returns all AvroSchemas that are present in a target DB */
    public List<AvroSchema> getAll(AvroConfig avroConfig) {
        return get(avroConfig, null);
    }

    /** Returns all AvroSchemas that are present in given DB schema */
    public List<AvroSchema> getForSchema(AvroConfig avroConfig, String dbSchemaName) {
        return get(avroConfig, dbSchemaName);
    }

    /** Returns AvroSchemas for each of given tables
     *
     * Note: when you have a schema with a large amount of tables but are only interested in a few of them,
     * consider calling getForTable() several times (separately for every table). This might significantly speedup
     * the process.
     * */
    public List<AvroSchema> getForTables(AvroConfig avroConfig, String dbSchemaName, String... tableNames) {
        return get(avroConfig, dbSchemaName, tableNames);
    }

    /** Returns AvroSchema for a specific table. */
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

            SchemaCrawlerOptions options = newSchemaCrawlerOptions();
            options = options.withLoadOptions(LoadOptionsBuilder.builder().withSchemaInfoLevel(maximum()).toOptions());

            LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();

            if (dbSchemaName != null) {
                String schemaPattern = ".*((?i)" + dbSchemaName + ")";
                limitOptionsBuilder.includeSchemas(new RegularExpressionInclusionRule(schemaPattern));
            }
            if (tableNames.length == 1) {
                // when we have only one table name we can use server side filter to speed things up
                // downside: filter is case sensitive
                limitOptionsBuilder.tableNamePattern(tableNames[0]);
            }

            options = options.withLimitOptions(limitOptionsBuilder.toOptions());

            Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);

            List<Schema> dbSchemas = new ArrayList<>(catalog.getSchemas());
            if (dbSchemaName != null) {
                dbSchemas = dbSchemas.stream().filter(schema ->
                        dbSchemaName.equalsIgnoreCase(schema.getCatalogName()) ||
                        dbSchemaName.equalsIgnoreCase(schema.getFullName()) ||
                        dbSchemaName.equalsIgnoreCase(Identifiers.STANDARD.quoteName(schema.getFullName())))
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

    private boolean containsIgnoreCase(String[] array, String word) {
        for (String s : array) {
            if (s.equalsIgnoreCase(word)) {
                return true;
            }
        }
        return false;
    }
}
