package com.at.avro.integration;

import com.at.avro.AvroSchema;
import com.at.avro.DbSchemaExtractor;
import com.at.avro.SchemaGenerator;
import com.at.avro.config.AvroConfig;
import com.at.avro.config.FormatterConfig;
import com.at.avro.formatters.SchemaFormatter;
import com.at.avro.mappers.RemovePlural;
import com.at.avro.mappers.ToCamelCase;
import org.flywaydb.core.Flyway;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

import static helper.Utils.classPathResourceContent;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author artur@callfire.com
 */
public class HsqlIntegrationTest {

    private static final String CONNECTION_URL = "jdbc:hsqldb:mem:testcase;shutdown=true";

    private static Connection CONNECTION;

    private AvroConfig avroConfig = new AvroConfig("test.namespace");

    private DbSchemaExtractor extractor;

    @BeforeClass
    public static void setupClass() throws SQLException {

        CONNECTION = DriverManager.getConnection(CONNECTION_URL, "sa", "");

        Flyway.configure()
            .dataSource(CONNECTION_URL, "sa", "")
            .locations("classpath:hsql/db/migration")
            .load().migrate();
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        CONNECTION.close();
    }

    @Before
    public void setup() throws SQLException {
        extractor = new DbSchemaExtractor(CONNECTION_URL, "sa", "");
    }

    @Test
    public void testGenerationWithDefaultSettings() {
        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/default.avsc")));
    }

    @Test
    public void testGenerationWithAllNullableFields() {
        avroConfig.setNullableTrueByDefault(true);

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/all_nullable.avsc")));
    }

    @Test
    public void testGenerationAllFieldsDefaultNull() {
        avroConfig.setAllFieldsDefaultNull(true);

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/all_fields_default_null.avsc")));
    }

    @Test
    public void testSpecificFormatterConfig() {
        FormatterConfig formatterConfig = FormatterConfig.builder()
            .setIndent("    ")
            .setPrettyPrintFields(true)
            .build();

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema, formatterConfig), is(classPathResourceContent("/hsql/avro/pretty_print_bigger_indent.avsc")));
    }

    @Test
    public void testNonPrettyPrint() {
        FormatterConfig formatterConfig = FormatterConfig.builder()
            .setPrettyPrintSchema(false)
            .build();

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema, formatterConfig), is(classPathResourceContent("/hsql/avro/non_pretty_print.avsc")));
    }

    @Test
    public void testNameMappers() {
        Function<String, String> nameMapper = new ToCamelCase().andThen(new RemovePlural());
        avroConfig.setSchemaNameMapper(nameMapper).setFieldNameMapper(nameMapper);

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/camel_case.avsc")));
    }

    @Test
    public void testOverrideFormatter() {
        FormatterConfig formatterConfig = FormatterConfig.builder()
            .setFormatter(AvroSchema.class, new SchemaFormatter() {
                @Override
                public String toJson(AvroSchema avroSchema, FormatterConfig config) {
                    return super.toJson(avroSchema, config).replaceAll(" ", "").replaceAll("\n", "");
                }
            })
            .build();

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema, formatterConfig), is(classPathResourceContent("/hsql/avro/custom_formatter.avsc")));
    }

    @Test
    public void testCustomAddingProperties() {
        avroConfig.setAvroSchemaPostProcessor(((avroSchema, table) -> {
            avroSchema.addCustomProperty("test-propertyy", "test-value");
        }));

        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/custom_property.avsc")));
    }

    @Test
    public void testGetAllAvroSchemas() {
        List<AvroSchema> result = extractor.getAll(avroConfig);
        assertThat(result.size(), is(3));
    }

    @Test
    public void testGetAvroSchemasInExistingDbSchema() {
        List<AvroSchema> result = extractor.getForSchema(avroConfig, "public");
        assertThat(result.size(), is(3));
    }

    @Test(expected = RuntimeException.class)
    public void testGetAvroSchemasInMissingDbSchema() {
        extractor.getForSchema(avroConfig, "nonexisting");
    }

    @Test
    public void testGetAvroSchemasForTables() {
        List<AvroSchema> result = extractor.getForTables(avroConfig, "public", "test_records", "non_existing");
        assertThat(result.size(), is(1));
    }

    @Test
    public void testToString() {
        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_records");
        assertThat(avroSchema.toString(), is(
            "AvroSchema[" +
                "name='test_records', namespace='test.namespace', " +
                "fields=[" +
                "AvroField[name='id', type=AvroType[type=Primitive(int), nullable=false]], " +
                "AvroField[name='name', type=AvroType[type=Primitive(string), nullable=true]], " +
                "AvroField[name='created', type=AvroType[type=Date(long): timestamp-millis, nullable=false]], " +
                "AvroField[name='updated', type=AvroType[type=Date(long): timestamp-millis, nullable=true]], " +
                "AvroField[name='decimal_field', type=AvroType[type=Decimal(string): decimal[20:3], nullable=true]], " +
                "AvroField[name='other_decimal_field', type=AvroType[type=Decimal(string): decimal[128:0], nullable=true]]" +
                "], " +
                "customProperties={}" +
                "]"
        ));
    }
    
    @Test
    public void testToStringWithDoc() {
        avroConfig.setUseSqlCommentsAsDoc(true);
    
        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_comments");
        assertThat(avroSchema.toString(), is(
            "AvroSchema[" +
                "name='test_comments', namespace='test.namespace', doc='Table with comments.', " +
                "fields=[" +
                "AvroField[name='id', type=AvroType[type=Primitive(int), nullable=false], doc='Id for the test_comments table.'], " +
                "AvroField[name='name', type=AvroType[type=Primitive(string), nullable=true]], " +
                "AvroField[name='created', type=AvroType[type=Date(long): timestamp-millis, nullable=false]], " +
                "AvroField[name='updated', type=AvroType[type=Date(long): timestamp-millis, nullable=true]], " +
                "AvroField[name='decimal_field', type=AvroType[type=Decimal(string): decimal[20:3], nullable=true]], " +
                "AvroField[name='other_decimal_field', type=AvroType[type=Decimal(string): decimal[128:0], nullable=true]]" +
                "], " +
                "customProperties={}" +
                "]"
        ));
    }
    
    @Test
    public void testUseSqlCommentsAsDoc() {
        avroConfig.setUseSqlCommentsAsDoc(true);
        
        AvroSchema avroSchema = extractor.getForTable(avroConfig, "public", "test_comments");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/hsql/avro/use_sql_comments_as_doc.avsc")));
    }
    
}
