package com.github.hpgrahsl.flink.udfs.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimum viable, zero-dependency parser for converting various schema string definitions
 * into corresponding proper Flink DataTypes.
 *
 */
public class SchemaParser {

    private static final Pattern FIELD_PATTERN = Pattern.compile(
        "([a-zA-Z_][a-zA-Z0-9_]*)\\s+(.+)",
        Pattern.CASE_INSENSITIVE
    );

    private static final Pattern TYPE_PARAM_PATTERN = Pattern.compile(
        "([A-Z_]+)(?:\\(([^)]+)\\))?",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Parses a schema string and returns a Flink ROW DataType.
     *
     * @param schemaString Schema in format "field1 TYPE1, field2 TYPE2, ..."
     * @return Flink DataType representing the ROW structure
     */
    public static DataType parseSchema(String schemaString) {
        if (schemaString == null || schemaString.trim().isEmpty()) {
            throw new IllegalArgumentException("schema string cannot be null or empty");
        }

        List<DataTypes.Field> fields = new ArrayList<>();
        List<String> fieldDefs = splitByComma(schemaString);

        for (String fieldDef : fieldDefs) {
            fieldDef = fieldDef.trim();
            if (fieldDef.isEmpty()) {
                continue;
            }

            Matcher matcher = FIELD_PATTERN.matcher(fieldDef);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(
                    "invalid field definition: '" + fieldDef + "' - expected format: 'fieldName TYPE'"
                );
            }

            String fieldName = matcher.group(1);
            String typeString = matcher.group(2);

            DataType fieldType = parseType(typeString);
            fields.add(DataTypes.FIELD(fieldName, fieldType));
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("schema must contain at least one field");
        }

        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
    }

    /**
     * Splits a string by commas, but ignores commas inside angle brackets or parentheses.
     * Example: "a INT, b ARRAY<STRING>, c ROW<x INT, y STRING>, d DECIMAL(10,2)"
     *       -> ["a INT", "b ARRAY<STRING>", "c ROW<x INT, y STRING>", "d DECIMAL(10,2)"]
     */
    private static List<String> splitByComma(String input) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;     // track < >
        int parenthesisDepth = 0; // track ( )

        for (char c : input.toCharArray()) {
            if (c == '<') {
                bracketDepth++;
                current.append(c);
            } else if (c == '>') {
                bracketDepth--;
                current.append(c);
            } else if (c == '(') {
                parenthesisDepth++;
                current.append(c);
            } else if (c == ')') {
                parenthesisDepth--;
                current.append(c);
            } else if (c == ',' && bracketDepth == 0 && parenthesisDepth == 0) {
                // top-level comma -> split here
                result.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // add the last field
        if (current.length() > 0) {
            result.add(current.toString().trim());
        }

        return result;
    }

    /**
     * Parses a type string into a Flink DataType.
     * Can be used for simple types like "INT", "STRING" or complex types like "ARRAY<STRING>".
     * Supports parameterized types like VARCHAR(50), DECIMAL(10,2), CHAR(5), etc.
     */
    public static DataType parseType(String typeString) {
        typeString = typeString.trim();
        String typeStringUpper = typeString.toUpperCase();

        // check for complex types first (ARRAY, ROW, MAP) before extracting parameters
        if (typeStringUpper.startsWith("ROW<") || typeStringUpper.startsWith("STRUCT<")) {
            return parseRowType(typeString);
        } else if (typeStringUpper.startsWith("MAP<")) {
            return parseMapType(typeString);
        } else if (typeStringUpper.startsWith("ARRAY<")) {
            return parseArrayType(typeString);            
        }

        // extract type name and optional parameters
        Matcher matcher = TYPE_PARAM_PATTERN.matcher(typeStringUpper);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid type format: " + typeString);
        }

        String typeName = matcher.group(1);
        String params = matcher.group(2); // null if no parameters

        switch (typeName) {
            // character strings
            case "CHAR":
                if (params != null) {
                    int length = Integer.parseInt(params.trim());
                    return DataTypes.CHAR(length);
                }
                return DataTypes.CHAR(1);
            case "VARCHAR":
                if (params != null) {
                    int length = Integer.parseInt(params.trim());
                    return DataTypes.VARCHAR(length);
                }
                return DataTypes.STRING();
            case "STRING":
                return DataTypes.STRING();
            // binary strings
            case "BINARY":
                if (params != null) {
                    int length = Integer.parseInt(params.trim());
                    return DataTypes.BINARY(length);
                }
                return DataTypes.BINARY(1);
            case "VARBINARY":
                if (params != null) {
                    int length = Integer.parseInt(params.trim());
                    return DataTypes.VARBINARY(length);
                }
                return DataTypes.BYTES();
            case "BYTES":
                return DataTypes.BYTES();
            // exact numerics
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
                if (params != null) {
                    String[] parts = params.split(",");
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
                    return DataTypes.DECIMAL(precision, scale);
                }
                return DataTypes.DECIMAL(10, 0);
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            // approximate numerics
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            // date and time
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                if (params != null) {
                    int precision = Integer.parseInt(params.trim());
                    return DataTypes.TIME(precision);
                }
                return DataTypes.TIME(0);
            case "TIMESTAMP":
                if (params != null) {
                    int precision = Integer.parseInt(params.trim());
                    return DataTypes.TIMESTAMP(precision);
                }
                return DataTypes.TIMESTAMP(6);
            case "TIMESTAMP_LTZ":
                if (params != null) {
                    int precision = Integer.parseInt(params.trim());
                    return DataTypes.TIMESTAMP_LTZ(precision);
                }
                return DataTypes.TIMESTAMP_LTZ(6);
            // other data types
            case "BOOLEAN":
                return DataTypes.BOOLEAN();

            default:
                throw new IllegalArgumentException(
                    "Unsupported type: '" + typeString + "'. Supported types: " +
                    "CHAR(n), VARCHAR(n), STRING, " +
                    "BINARY(n), VARBINARY(n), BYTES, " +
                    "DECIMAL(p,s), DEC(p,s), NUMERIC(p,s), TINYINT, SMALLINT, INT, BIGINT, " +
                    "FLOAT, DOUBLE, " +
                    "DATE, TIME(p), TIMESTAMP(p), TIMESTAMP_LTZ(p), " +
                    "BOOLEAN, " +
                    "ARRAY<T>, ROW<...>, MAP<K,V>"
                );
        }
    }

    /**
     * Parses ARRAY<TYPE> format.
     */
    private static DataType parseArrayType(String typeString) {
        // extract element type from ARRAY<TYPE> handling nested brackets
        if (!typeString.toUpperCase().startsWith("ARRAY<")) {
            throw new IllegalArgumentException("Invalid ARRAY type format: " + typeString);
        }
        String content = extractBracketedContent(typeString, "ARRAY".length());
        if (content == null) {
            throw new IllegalArgumentException("Invalid ARRAY type format: " + typeString);
        }

        DataType elementType = parseType(content.trim());
        return DataTypes.ARRAY(elementType);
    }

    /**
     * Parses MAP<KEY_TYPE, VALUE_TYPE> format.
     */
    private static DataType parseMapType(String typeString) {
        // extract key and value types from MAP<KEY_TYPE, VALUE_TYPE>
        if (!typeString.toUpperCase().startsWith("MAP<")) {
            throw new IllegalArgumentException("Invalid MAP type format: " + typeString);
        }

        String content = extractBracketedContent(typeString, "MAP".length());
        if (content == null) {
            throw new IllegalArgumentException("Invalid MAP type format: " + typeString);
        }

        // split by comma (respecting nested brackets)
        List<String> parts = splitByComma(content);
        if (parts.size() != 2) {
            throw new IllegalArgumentException(
                "MAP requires exactly 2 type parameters: KEY_TYPE, VALUE_TYPE - got: " + content
            );
        }

        DataType keyType = parseType(parts.get(0).trim());
        DataType valueType = parseType(parts.get(1).trim());

        return DataTypes.MAP(keyType, valueType);
    }

    /**
     * Parses ROW<field1 TYPE1, field2 TYPE2, ...> format.
     */
    private static DataType parseRowType(String typeString) {
        // extract content from ROW<...> or STRUCT<...>
        String upper = typeString.toUpperCase();
        int offset;
        if (upper.startsWith("ROW<")) {
            offset = "ROW".length();
        } else if (upper.startsWith("STRUCT<")) {
            offset = "STRUCT".length();
        } else {
            throw new IllegalArgumentException("invalid ROW type format: " + typeString);
        }

        String content = extractBracketedContent(typeString, offset);
        if (content == null) {
            throw new IllegalArgumentException("invalid ROW type format: " + typeString);
        }

        return parseSchema(content.trim());
    }

    /**
     * Extracts content between balanced angle brackets.
     * Example: extractBracketedContent("ARRAY<ROW<a INT>>", 5) returns "ROW<a INT>"
     *
     * @param input The input string
     * @param offset The position of the opening '<' bracket
     * @return The content between balanced brackets, or null if brackets are not balanced
     */
    private static String extractBracketedContent(String input, int offset) {
        if (offset >= input.length() || input.charAt(offset) != '<') {
            return null;
        }

        int bracketDepth = 1;
        int start = offset + 1;
        int i = start;

        while (i < input.length() && bracketDepth > 0) {
            char c = input.charAt(i);
            if (c == '<') {
                bracketDepth++;
            } else if (c == '>') {
                bracketDepth--;
            }
            i++;
        }

        if (bracketDepth != 0) {
            return null; // Unbalanced brackets
        }

        return input.substring(start, i - 1);
    }

    /**
     * Extracts field names from a ROW DataType in order.
     */
    public static List<String> getFieldNames(DataType rowType) {
        List<String> fieldNames = new ArrayList<>();
        if (rowType.getLogicalType() instanceof RowType) {
            RowType logicalRowType = (RowType)rowType.getLogicalType();
            fieldNames.addAll(logicalRowType.getFieldNames());
        }
        return fieldNames;
    }
}
