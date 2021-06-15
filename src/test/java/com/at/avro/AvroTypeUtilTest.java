package com.at.avro;

import com.at.avro.config.AvroConfig;
import com.at.avro.types.Date;
import com.at.avro.types.Decimal;
import com.at.avro.types.Primitive;
import org.junit.Test;
import schemacrawler.schema.Column;
import schemacrawler.schema.ColumnDataType;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test db to avro types mapping
 *
 * @author artur@callfire.com
 */
public class AvroTypeUtilTest {

    @Test
    public void testPrimitives() {
        validatePrimitiveType("int", "int");
        validatePrimitiveType("int2", "int");
        validatePrimitiveType("int4", "int");
        validatePrimitiveType("integer", "int");
        validatePrimitiveType("smallint", "int");
        validatePrimitiveType("tinyint", "int");
        validatePrimitiveType("serial", "int");
        validatePrimitiveType("smallserial", "int");

        validatePrimitiveType("tinyblob", "bytes");
        validatePrimitiveType("blob", "bytes");
        validatePrimitiveType("binary", "bytes");
        validatePrimitiveType("varbinary", "bytes");
        validatePrimitiveType("longvarbinary", "bytes");

        validatePrimitiveType("bigserial", "long");
        validatePrimitiveType("bigint", "long");
        validatePrimitiveType("int8", "long");

        validatePrimitiveType("bit", "boolean");
        validatePrimitiveType("bool", "boolean");
        validatePrimitiveType("boolean", "boolean");

        validatePrimitiveType("nchar", "string");
        validatePrimitiveType("char", "string");
        validatePrimitiveType("varchar", "string");
        validatePrimitiveType("longtext", "string");
        validatePrimitiveType("longvarchar", "string");
        validatePrimitiveType("longnvarchar", "string");
        validatePrimitiveType("nvarchar", "string");
        validatePrimitiveType("bpchar", "string");
        validatePrimitiveType("inet", "string");
        validatePrimitiveType("macaddr", "string");
        validatePrimitiveType("cidr", "string");
        validatePrimitiveType("uuid", "string");
        validatePrimitiveType("xml", "string");
        validatePrimitiveType("json", "string");

        validatePrimitiveType("double precision", "double");
        validatePrimitiveType("double", "double");
        validatePrimitiveType("float", "double");
        validatePrimitiveType("float8", "double");

        validatePrimitiveType("real", "float");
        validatePrimitiveType("float4", "float");
        validatePrimitiveType("int unsigned", "int");
        validatePrimitiveType("tinyint unsigned", "int");
        validatePrimitiveType("bigint unsigned", "long");
        validatePrimitiveType("smallint unsigned", "int");
        validatePrimitiveType("mediumint", "int");
        validatePrimitiveType("mediumint unsigned", "int");
    }

    @Test
    public void testDateTypes() throws Exception {
        String[] dateTypes = new String[] { "date", "time", "datetime", "timestamp", "timestamptz" };

        for (String dateType : dateTypes) {
            AvroType avroType = AvroTypeUtil.getAvroType(column(dateType), defaultConfig());
            assertThat(avroType.getType(), instanceOf(Date.class));
            assertThat(avroType.getType().getPrimitiveType(), is("long"));
            assertThat(((Date) avroType.getType()).getLogicalType(), is("timestamp-millis"));
        }
    }

    @Test
    public void testDecimalTypes() throws Exception {
        String[] dateTypes = new String[] { "decimal", "numeric" };

        for (String dateType : dateTypes) {
            Column column = column(dateType);
            when(column.getSize()).thenReturn(20);
            when(column.getDecimalDigits()).thenReturn(3);

            AvroType avroType = AvroTypeUtil.getAvroType(column, defaultConfig());
            assertThat(avroType.getType(), instanceOf(Decimal.class));
            assertThat(avroType.getType().getPrimitiveType(), is("string"));
            assertThat(((Decimal) avroType.getType()).getLogicalType(), is("decimal"));
            assertThat(((Decimal) avroType.getType()).getPrecision(), is(20));
            assertThat(((Decimal) avroType.getType()).getScale(), is(3));
            assertThat(((Decimal) avroType.getType()).getJavaClass(), is("java.math.BigDecimal"));
        }
    }

    @Test
    public void testUserDefinedType() throws Exception {
        Column column = column("test");
        when(column.getType().isUserDefined()).thenReturn(true);

        AvroType avroType = AvroTypeUtil.getAvroType(column, defaultConfig());
        assertThat(avroType.getType(), instanceOf(Primitive.class));
        assertThat(avroType.getType().getPrimitiveType(), is("string"));
    }

    @Test
    public void testOverrideNullable() throws Exception {
        AvroConfig avroConfig = defaultConfig().setNullableTrueByDefault(true);
        AvroType avroType = AvroTypeUtil.getAvroType(column("int"), avroConfig);

        assertThat(avroType.isNullable(), is(true));
    }

    @Test
    public void testEnumsAsStrings() throws Exception {
        AvroConfig avroConfig = defaultConfig().setRepresentEnumsAsStrings(true);
        AvroType avroType = AvroTypeUtil.getAvroType(column("enum"), avroConfig);

        assertThat(avroType.getType(), instanceOf(Primitive.class));
        assertThat(avroType.getType().getPrimitiveType(), is("string"));
    }

    private void validatePrimitiveType(String dbType, String expectedPrimitive) {
        AvroType avroType = AvroTypeUtil.getAvroType(column(dbType), defaultConfig());

        assertThat(avroType.getType(), instanceOf(Primitive.class));
        assertThat(avroType.getType().getPrimitiveType(), is(expectedPrimitive));
        assertThat(avroType.isNullable(), is(false));
    }

    @Test
    public void testUnknownTypeResolver() throws Exception {
        AvroConfig avroConfig = defaultConfig().setUnknownTypeResolver(type -> "string");
        AvroType avroType = AvroTypeUtil.getAvroType(column("wtf"), avroConfig);

        assertThat(avroType.getType(), instanceOf(Primitive.class));
        assertThat(avroType.getType().getPrimitiveType(), is("string"));
    }

    private AvroConfig defaultConfig() {
        return new AvroConfig("test");
    }

    private Column column(String dbType) {
        Column column = mock(Column.class, RETURNS_DEEP_STUBS);
        ColumnDataType columnDataType = mock(ColumnDataType.class);

        when(columnDataType.getName()).thenReturn(dbType);
        when(column.getType()).thenReturn(columnDataType);

        return column;
    }
}