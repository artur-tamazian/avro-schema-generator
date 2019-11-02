package com.at.avro.integration;

import com.at.avro.AvroSchema;
import com.at.avro.DbSchemaExtractor;
import com.at.avro.SchemaGenerator;
import com.at.avro.config.AvroConfig;
import org.flywaydb.core.Flyway;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;

import static helper.Utils.classPathResourceContent;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PgsqlIntegrationTest {

    @ClassRule
    public static PostgreSQLContainer PGSQL = new PostgreSQLContainer();

    final static private AvroConfig AVRO_CONFIG = new AvroConfig("pgsql");

    static private DbSchemaExtractor SCHEMA_EXTRACTOR;

    @BeforeClass
    public static void setupClass() {
        Flyway.configure()
            .dataSource(PGSQL.getJdbcUrl(), PGSQL.getUsername(), PGSQL.getPassword())
            .locations("classpath:pgsql/db/migration")
            .load().migrate();

        SCHEMA_EXTRACTOR = new DbSchemaExtractor(PGSQL.getJdbcUrl(), PGSQL.getUsername(), PGSQL.getPassword());
    }

    @Test
    public void testDefaultTabe() {
        AvroSchema avroSchema = SCHEMA_EXTRACTOR.getForTable(AVRO_CONFIG, null, "default_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/default_table.avsc")));
    }

    @Test
    public void testArrayTable() {
        AvroSchema avroSchema = SCHEMA_EXTRACTOR.getForTable(AVRO_CONFIG, null, "array_table");
        assertThat(SchemaGenerator.generate(avroSchema), is(classPathResourceContent("/pgsql/avro/array_table.avsc")));
    }
}
