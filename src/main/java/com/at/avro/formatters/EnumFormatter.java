package com.at.avro.formatters;

import com.at.avro.config.FormatterConfig;
import com.at.avro.types.Enum;

/**
 * @author artur@callfire.com
 */
public class EnumFormatter implements Formatter<Enum> {
    @Override
    public String toJson(Enum anEnum, FormatterConfig config) {
        String template = "{ \"type\":\"enum\", \"name\":\"%s\", \"symbols\":[%s] }".replaceAll(":", config.colon());
        return String.format(template, anEnum.getName(), formatSymbols(anEnum.getSymbols(), config));
    }

    private String formatSymbols(String[] symbols, FormatterConfig config) {
        StringBuilder symbolsJson = new StringBuilder();
        if (symbols.length > 0) {
            for (String symbol : symbols) {
                symbolsJson.append("\"").append(symbol.trim()).append("\", ");
            }
            symbolsJson.setLength(symbolsJson.length() - 2);
        }
        return symbolsJson.toString();
    }
}
