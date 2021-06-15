package com.at.avro;

import com.at.avro.config.AvroConfig;
import com.at.avro.types.Enum;
import com.at.avro.types.*;
import schemacrawler.schema.Column;

import static java.util.Arrays.asList;

/**
 * @author artur@callfire.com
 */
final class AvroTypeUtil {

    /**
     * Maps db Columns to AvroTypes.
     * Mapping logic can be tweaked a bit using AvroConfig.
     */
    static AvroType getAvroType(Column column, AvroConfig config) {
        boolean nullable = column.isNullable() || config.isNullableTrueByDefault();
        String type = column.getType().getName().toLowerCase();
        Class typeClz = column.getType().getTypeMappedClass();

        if (type.equalsIgnoreCase("enum")) {
            if (config.representEnumsAsStrings()) {
                return new AvroType(new Primitive("string"), nullable);
            } else {
                return new AvroType(new Enum(column), nullable);
            }
        }
        else if (column.getType().isUserDefined()) {
            return new AvroType(new Primitive("string"), nullable);
        }
        else if (asList("decimal", "numeric").contains(type)) {
            return new AvroType(new Decimal(column, config), nullable);
        }
        else if (asList("timestamptz", "timestamp", "datetime", "date", "time").contains(type)) {
            return new AvroType(new Date(column, config), nullable);
        } else if (typeClz == java.sql.Array.class) {
            return new AvroType(new Array(new Primitive(getPrimitiveType(type.substring(1), config))), nullable);
        }

        return new AvroType(new Primitive(getPrimitiveType(type, config)), nullable);
    }

    private static String getPrimitiveType(String type, AvroConfig config) {
        switch (type.toLowerCase()) {

            case "integer":
            case "int":
            case "int unsigned":
            case "int2":
            case "int4":
            case "mediumint":
            case "mediumint unsigned":
            case "smallint":
            case "smallint unsigned":
            case "serial":
            case "smallserial":
            case "tinyint":
            case "tinyint unsigned": return "int";

            case "tinyblob":
            case "blob":
            case "binary":
            case "varbinary":
            case "longvarbinary": return "bytes";

            case "int8":
            case "bigserial":
            case "bigint":
            case "bigint unsigned": return "long";

            case "bit":
            case "bool":
            case "boolean": return "boolean";

            case "char":
            case "bpchar":
            case "varchar":
            case "text":
            case "longtext":
            case "longvarchar":
            case "longnvarchar":
            case "nvarchar":
            case "macaddr":
            case "inet":
            case "cidr":
            case "uuid":
            case "xml":
            case "json":
            case "nchar": return "string";

            case "double precision":
            case "float":
            case "float8":
            case "double": return "double";

            case "float4":
            case "real": return "float";

            default:
                return config.getUnknownTypeResolver().apply(type);
        }
    }
}
