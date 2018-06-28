package com.at.avro.mappers;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author artur@callfire.com
 */
public class ToCamelCaseTest {

    @Test
    public void testSimpleWord() throws Exception {
        assertThat(new ToCamelCase().apply("word"), is("Word"));
    }

    @Test
    public void testUnderscores() throws Exception {
        assertThat(new ToCamelCase().apply("some_word"), is("SomeWord"));
        assertThat(new ToCamelCase().apply("_word"), is("Word"));
    }
}