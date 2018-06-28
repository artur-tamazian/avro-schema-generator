package com.at.avro.mappers;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author artur@callfire.com
 */
public class RemovePluralTest {

    @Test
    public void testNonPlural() throws Exception {
        assertThat(new RemovePlural().apply("word"), is("word"));
    }

    @Test
    public void testPlural() throws Exception {
        assertThat(new RemovePlural().apply("words"), is("word"));
    }
}