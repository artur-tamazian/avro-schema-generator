package com.at.avro.integration;

import com.at.avro.AvroSchema;
import com.at.avro.DbSchemaExtractor;
import com.at.avro.SchemaGenerator;
import com.at.avro.config.AvroConfig;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Comparator;
import java.util.List;

import static helper.Utils.classPathResourceContent;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PgsqlIntegrationTest {

    @ClassRule
    public static PostgreSQLContainer PGSQL = new PostgreSQLContainer();

    private AvroConfig avroConfig = new AvroConfig("pgsql");

    private DbSchemaExtractor extractor;

    @BeforeClass
    public static void setupClass() {
        Flyway.configure()
            .dataSource(PGSQL.getJdbcUrl(), PGSQL.getUsername(), PGSQL.getPassword())
            .locations("classpath:pgsql/db/migration")
            .load().migrate();
    }

    @Before
    public void setup() {
        extractor = new DbSchemaExtractor(PGSQL.getJdbcUrl(), PGSQL.getUsername(), PGSQL.getPassword());
    }

    @Test
    public void testDefaultTable() {
        AvroSchema avroSchema = extractor.getForTable(avroConfig, null, "default_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/default_table.avsc")));
    }

    @Test
    public void testArrayTable() {
        AvroSchema avroSchema = extractor.getForTable(avroConfig, null, "array_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/array_table.avsc")));
    }

    @Test
    public void testDefaultTableWithDoc() {
        avroConfig.setUseSqlCommentsAsDoc(true);
        AvroSchema avroSchema = extractor.getForTable(avroConfig, null, "comment_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/comment_table.avsc")));
    }

    @Test
    public void testMultipleTables() {
        List<AvroSchema> schemas = extractor.getForTables(avroConfig, null, "default_table", "comment_table", "array_table");
        assertThat(schemas.size(), is(3));
        AvroSchema arrayTableSchema = schemas.stream().sorted(Comparator.comparing(schema -> schema.getName())).findFirst().get();
        assertThat(arrayTableSchema.getFields().size(), is(7));
    }

    @Test
    public void testSchemaWithDashes() {
        AvroSchema avroSchema = extractor.getForTable(avroConfig, "\"test-schema\"", "small_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/small_table.avsc")));
    }
}
