package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.types.Enum;
import org.junit.Test;
import schemacrawler.schema.Column;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Art
 */
public class EnumFormatterTest {

    @Test
    public void testEnumFormatting() throws Exception {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("someEnum");
        when(column.getAttribute("COLUMN_TYPE")).thenReturn("ENUM('value1', 'value2')");

        Enum anEnum = new Enum(column);

        assertThat(anEnum.getName(), is("someEnum"));
        assertThat(anEnum.getSymbols()[0], is("value1"));
        assertThat(anEnum.getSymbols()[1], is("value2"));

        String json = new EnumFormatter().toJson(anEnum, FormatterConfig.builder().build());
        assertThat(json, is("{ \"type\": \"enum\", \"name\": \"someEnum\", \"symbols\": [\"value1\", \"value2\"] }"));
    }
}