/*
 * Copyright (c) 2025. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.hpgrahsl.flink.udfs.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Custom function to parse JSON objects or arrays with explicit target schema
 * definition.
 * Inspired by Databricks' `from_json` function
 * (https://docs.databricks.com/aws/en/sql/language-manual/functions/from_json)
 * 
 * Usage:
 * SELECT FROM_JSON('[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]',
 * 'ARRAY<ROW<id INT, name STRING>>')
 * Returns:
 * ARRAY<ROW<id INT, name STRING>>
 */
public class FromJsonUdf extends ScalarFunction {

    private static final int SCHEMA_LRU_CACHE_SIZE = 32;

    private transient ObjectMapper objectMapper;

    // used as LRU cache for parsed schemas (schema string -> parsed DataType)
    private transient Map<String, DataType> schemaCache;

    private transient boolean isInitialized = false;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initialize();
    }

    /**
     * Parses JSON into properly typed Flink data structures based on the given JSON string
     * and the specified target schema. Supports both individual JSON objects and JSON arrays.
     *
     * @param jsonString   the JSON string to parse (can be a JSON object or JSON array); if null or empty, returns null
     * @param schemaString the schema definition string specifying the target data type:
     *                     <ul>
     *                       <li>For JSON arrays: {@code "ARRAY<element_type>"} or {@code "ARRAY<ROW<field1 TYPE1, ...>>"}</li>
     *                       <li>For JSON objects as rows: {@code "ROW<field1 TYPE1, field2 TYPE2, ...>"}</li>
     *                       <li>For JSON objects as maps: {@code "MAP<KEY_TYPE, VALUE_TYPE>"}</li>
     *                     </ul>
     * @return a {@link Row} for a single JSON object with ROW schema, a {@link Map} for a JSON object with MAP schema,
     *         or an array (Object[]) for a JSON array; returns null if jsonString is null or empty
     * @throws IllegalArgumentException if schemaString is null or empty, if the schema format doesn't match the JSON structure,
     *                                  or if the JSON structure is neither an object nor an array
     * @throws RuntimeException if JSON parsing fails
     */
    public Object eval(String jsonString, String schemaString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return null;
        }

        if (schemaString == null || schemaString.trim().isEmpty()) {
            throw new IllegalArgumentException("schemaString parameter cannot be null or empty");
        }

        if (!isInitialized) {
            initialize();
        }

        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            String schema = schemaString.trim().toUpperCase();
            if (rootNode.isObject()) {
                if (schema.startsWith("ROW<")) {
                    return parseObject(rootNode, schemaString);
                } else if (schema.startsWith("MAP<")) {
                    return parseMap(rootNode, schemaString);
                } else {
                    throw new IllegalArgumentException(
                            "for JSON objects, schema must start with ROW<...> or MAP<...> - got: " + schemaString);
                }
            } else if (rootNode.isArray()) {
                if (!schema.startsWith("ARRAY<")) {
                    throw new IllegalArgumentException(
                            "for JSON arrays, schema must start with ARRAY<...> - got: " + schemaString);
                }
                return parseArray(rootNode, schemaString);
            } else {
                throw new IllegalArgumentException("jsonString must represent either a JSON object or a JSON array");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("failed to parse JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Initializes the function by creating the ObjectMapper and schema cache.
     * This method provides an alternative initialization path for contexts where the
     * standard Flink lifecycle hooks (such as {@link #open(FunctionContext)}) are not
     * automatically invoked, such as when using this function in Flink CDC pipelines.
     */
    private void initialize() {
        objectMapper = new ObjectMapper();
        schemaCache = Collections.synchronizedMap(
                new LinkedHashMap<String, DataType>(SCHEMA_LRU_CACHE_SIZE, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, DataType> eldest) {
                        return size() > SCHEMA_LRU_CACHE_SIZE;
                    }
                });
        isInitialized = true;
    }

    /**
     * Retrieves a cached parsed schema or parses and caches it if not present.
     * The cache uses LRU (Least Recently Used) eviction policy when it reaches
     * the maximum size of {@value #SCHEMA_LRU_CACHE_SIZE} entries.
     *
     * @param schemaString the schema definition string to parse and cache
     * @return the parsed {@link DataType} corresponding to the schema string
     */
    private DataType getCachedSchema(String schemaString) {
        return schemaCache.computeIfAbsent(schemaString, SchemaParser::parseType);
    }

    /**
     * Parses a single JSON object into a Flink {@link Row}.
     *
     * @param jsonObject   the JSON object node to parse
     * @param schemaString the schema definition string with format {@code "ROW<field1 TYPE1, field2 TYPE2, ...>"}
     * @return a {@link Row} containing the parsed field values in the order specified by the schema
     */
    private Row parseObject(JsonNode jsonObject, String schemaString) {
        DataType rowType = getCachedSchema(schemaString);
        List<String> fieldNames = SchemaParser.getFieldNames(rowType);
        RowType logicalRowType = (RowType) rowType.getLogicalType();
        Row row = new Row(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            JsonNode fieldValue = jsonObject.get(fieldName);
            LogicalType fieldType = logicalRowType.getTypeAt(i);
            row.setField(i, convertJsonNodeToValue(fieldValue, fieldType));
        }
        return row;
    }

    /**
     * Parses a JSON object into a {@link Map}.
     * JSON object keys (which are always strings) are converted to the target key type specified in the schema.
     *
     * @param jsonObject   the JSON object node to parse
     * @param schemaString the schema definition string with format {@code "MAP<KEY_TYPE, VALUE_TYPE>"}
     * @return a {@link Map} containing the parsed key-value pairs with types as specified in the schema
     * @throws IllegalArgumentException if the schema is not a MAP type
     */
    private Map<Object, Object> parseMap(JsonNode jsonObject, String schemaString) {
        DataType mapType = getCachedSchema(schemaString);
        if (!(mapType.getLogicalType() instanceof MapType)) {
            throw new IllegalArgumentException("Schema must be a MAP type, got: " + schemaString);
        }

        MapType logicalMapType = (MapType) mapType.getLogicalType();
        LogicalType keyType = logicalMapType.getKeyType();
        LogicalType valueType = logicalMapType.getValueType();

        Map<Object, Object> result = new HashMap<>();
        jsonObject.fields().forEachRemaining(entry -> {
            // JSON object keys are always strings, convert to target key type
            Object key = convertStringToType(entry.getKey(), keyType);
            Object value = convertJsonNodeToValue(entry.getValue(), valueType);
            result.put(key, value);
        });

        return result;
    }

    /**
     * Parses a JSON array into a typed Java array.
     *
     * @param jsonArray    the JSON array node to parse
     * @param schemaString the schema definition string with format {@code "ARRAY<element_type>"} where element_type can be:
     *                     <ul>
     *                       <li>A simple type: {@code "ARRAY<INT>"}, {@code "ARRAY<STRING>"}</li>
     *                       <li>A ROW type: {@code "ARRAY<ROW<field1 TYPE1, field2 TYPE2>>"}</li>
     *                       <li>A nested ARRAY: {@code "ARRAY<ARRAY<...>>"}</li>
     *                       <li>A MAP type: {@code "ARRAY<MAP<KEY_TYPE, VALUE_TYPE>>"}</li>
     *                     </ul>
     * @return a typed array (Object[]) containing the parsed elements with types as specified in the schema
     * @throws IllegalArgumentException if the schema is not an ARRAY type
     */
    private Object parseArray(JsonNode jsonArray, String schemaString) {
        DataType arrayType = getCachedSchema(schemaString);
        if (!(arrayType.getLogicalType() instanceof ArrayType)) {
            throw new IllegalArgumentException("schema must be an ARRAY type, got: " + schemaString);
        }

        ArrayType logicalArrayType = (ArrayType) arrayType.getLogicalType();
        LogicalType elementType = logicalArrayType.getElementType();

        if (jsonArray.size() == 0) {
            return createTypedArray(Collections.emptyList(), elementType);
        }

        List<Object> result = new ArrayList<>(jsonArray.size());
        for (JsonNode element : jsonArray) {
            result.add(convertJsonNodeToValue(element, elementType));
        }

        return createTypedArray(result, elementType);
    }

    /**
     * Converts a JsonNode to a Java object based on the target Flink logical type.
     * Handles the most common Flink data types including primitives, date/time types, arrays, rows, and maps.
     *
     * @param node       the JSON node to convert; if null or represents a JSON null value, returns null
     * @param targetType the target Flink {@link LogicalType} to convert the node to
     * @return the converted Java object matching the target type, or null if the node is null
     */
    private Object convertJsonNodeToValue(JsonNode node, LogicalType targetType) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (targetType.getTypeRoot()) {
            case BOOLEAN:
                return node.asBoolean();

            case TINYINT:
                return (byte) node.asInt();
            case SMALLINT:
                return (short) node.asInt();
            case INTEGER:
                return node.asInt();
            case BIGINT:
                return node.asLong();

            case FLOAT:
                return (float) node.asDouble();
            case DOUBLE:
                return node.asDouble();
            case DECIMAL:
                return node.decimalValue();

            case CHAR:
            case VARCHAR:
                return node.asText();

            case BINARY:
            case VARBINARY:
                try {
                    return node.binaryValue();
                } catch (Exception e) {
                    return node.asText().getBytes();
                }

            case DATE:
                return LocalDate.parse(node.asText());

            case TIME_WITHOUT_TIME_ZONE:
                return parseLocalTime(node.asText());

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (node.isNumber()) {
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(node.asLong()), ZoneOffset.UTC);
                }
                return parseLocalDateTime(node.asText());

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (node.isNumber()) {
                    return Instant.ofEpochMilli(node.asLong());
                }
                return parseInstant(node.asText());

            case ARRAY:
                ArrayType arrayType = (ArrayType) targetType;
                LogicalType elementType = arrayType.getElementType();

                List<Object> arrayList = new ArrayList<>();
                for (JsonNode element : node) {
                    arrayList.add(convertJsonNodeToValue(element, elementType));
                }
                return createTypedArray(arrayList, elementType);

            case ROW:
                RowType rowType = (RowType) targetType;
                List<String> nestedFieldNames = rowType.getFieldNames();
                Row nestedRow = new Row(nestedFieldNames.size());
                for (int i = 0; i < nestedFieldNames.size(); i++) {
                    String fieldName = nestedFieldNames.get(i);
                    JsonNode fieldValue = node.get(fieldName);
                    LogicalType fieldType = rowType.getTypeAt(i);
                    nestedRow.setField(i, convertJsonNodeToValue(fieldValue, fieldType));
                }
                return nestedRow;

            case MAP:
                MapType mapType = (MapType) targetType;
                LogicalType keyType = mapType.getKeyType();
                LogicalType valueType = mapType.getValueType();
                Map<Object, Object> map = new HashMap<>();
                node.fields().forEachRemaining(entry -> {
                    // JSON object keys are always strings, convert to target key type
                    Object key = convertStringToType(entry.getKey(), keyType);
                    Object value = convertJsonNodeToValue(entry.getValue(), valueType);
                    map.put(key, value);
                });
                return map;

            default:
                // for unsupported types, fallback to string
                return node.asText();
        }
    }

    /**
     * Converts a string value to the target Flink logical type.
     * This method is primarily used for converting JSON object keys (which are always strings)
     * to the appropriate key type when parsing MAP types, since MAP keys can be of none-string types.
     *
     * @param str        the string value to convert; if null, returns null
     * @param targetType the target Flink {@link LogicalType} to convert the string to
     * @return the converted Java object matching the target type, or null if str is null;
     *         for unsupported types, returns the original string value
     */
    private Object convertStringToType(String str, LogicalType targetType) {
        if (str == null) {
            return null;
        }

        switch (targetType.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.parseBoolean(str);

            case TINYINT:
                return Byte.parseByte(str);
            case SMALLINT:
                return Short.parseShort(str);
            case INTEGER:
                return Integer.parseInt(str);
            case BIGINT:
                return Long.parseLong(str);

            case FLOAT:
                return Float.parseFloat(str);
            case DOUBLE:
                return Double.parseDouble(str);

            case CHAR:
            case VARCHAR:
                return str;

            case DATE:
                return LocalDate.parse(str);

            case TIME_WITHOUT_TIME_ZONE:
                return parseLocalTime(str);

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                try {
                    return parseLocalDateTime(str);
                } catch (IllegalArgumentException e) {
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(str)), ZoneOffset.UTC);
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                try {
                    return parseInstant(str);
                } catch (IllegalArgumentException e) {
                    return Instant.ofEpochMilli(Long.parseLong(str));
                }

            default:
                // for unsupported key types, fallback to string
                return str;
        }
    }

    /**
     * Creates a typed Java array from a list of elements based on the specified element type.
     * Uses reflection to create an array of the appropriate type for the given logical type.
     *
     * @param elements    the list of elements to convert into an array
     * @param elementType the Flink {@link LogicalType} of the array elements
     * @return a typed array (e.g., Integer[], String[], Row[]) containing the elements
     */
    private Object createTypedArray(List<Object> elements, LogicalType elementType) {
        Class<?> elementClass = getDefaultConversionClass(elementType);
        Object typedArray = Array.newInstance(elementClass, elements.size());
        for (int i = 0; i < elements.size(); i++) {
            Array.set(typedArray, i, elements.get(i));
        }
        return typedArray;
    }

    /**
     * Determines the default Java class used for converting values of a given Flink logical type.
     * This mapping is used when creating typed arrays to ensure the correct array component type.
     *
     * @param logicalType the Flink {@link LogicalType} to get the conversion class for
     * @return the Java {@link Class} that represents the default conversion target for the logical type;
     *         returns {@link Object}.class for unsupported or unknown types
     */
    private Class<?> getDefaultConversionClass(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DECIMAL:
                return BigDecimal.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DATE:
                return LocalDate.class;
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return LocalDateTime.class;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Instant.class;
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                Class<?> childClass = getDefaultConversionClass(arrayType.getElementType());
                return Array.newInstance(childClass, 0).getClass();
            case ROW:
                return Row.class;
            case MAP:
                return Map.class;
            default:
                return Object.class;
        }
    }

    /**
     * Parses a string value into a {@link LocalTime} object.
     * Attempts ISO-8601 format first, then falls back to SQL TIME format if that fails.
     *
     * @param value the string representation of a time value
     * @return the parsed {@link LocalTime} object
     * @throws DateTimeParseException if the value cannot be parsed as a time
     */
    private LocalTime parseLocalTime(String value) {
        try {
            return LocalTime.parse(value);
        } catch (DateTimeParseException e) {
            return java.sql.Time.valueOf(value).toLocalTime();
        }
    }

    /**
     * Parses a string value into a {@link LocalDateTime} object.
     * Attempts multiple formats in order:
     * <ol>
     *   <li>ISO-8601 format (e.g., "2023-01-01T12:30:00")</li>
     *   <li>Space-separated format normalized to ISO-8601 (e.g., "2023-01-01 12:30:00")</li>
     *   <li>SQL TIMESTAMP format</li>
     * </ol>
     *
     * @param value the string representation of a date-time value
     * @return the parsed {@link LocalDateTime} object
     * @throws IllegalArgumentException if the value cannot be parsed as a date-time in any supported format
     */
    private LocalDateTime parseLocalDateTime(String value) {
        try {
            return LocalDateTime.parse(value);
        } catch (DateTimeParseException ignored) {
        }

        String normalized = value.replace(' ', 'T');
        try {
            return LocalDateTime.parse(normalized);
        } catch (DateTimeParseException ignored) {
        }

        return java.sql.Timestamp.valueOf(value).toLocalDateTime();
    }

    /**
     * Parses a string value into an {@link Instant} object.
     * Attempts ISO-8601 instant format first (e.g., "2023-01-01T12:30:00Z").
     * If that fails, parses as a {@link LocalDateTime} and converts to an instant using UTC timezone.
     *
     * @param value the string representation of an instant value
     * @return the parsed {@link Instant} object
     * @throws DateTimeParseException if the value cannot be parsed as an instant or date-time
     */
    private Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            LocalDateTime ldt = parseLocalDateTime(value);
            return ldt.atZone(ZoneOffset.UTC).toInstant();
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // two string arguments: JSON string and schema
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.STRING()),
                        InputTypeStrategies.explicit(DataTypes.STRING())))
                // output type is determined entirely from the schema parameter given as string
                // literal
                .outputTypeStrategy(callContext -> {
                    if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                        throw new IllegalArgumentException(
                                "2nd argument (schema) must be a string literal, not a column reference or expression");
                    }
                    Optional<String> schemaStringOpt = callContext.getArgumentValue(1, String.class);
                    if (!schemaStringOpt.isPresent()) {
                        throw new IllegalArgumentException("schema parameter must be a non-null string literal");
                    }

                    String schemaString = schemaStringOpt.get();
                    String schemaUpper = schemaString.trim().toUpperCase();
                    // schema must be one of ROW<...> or MAP<...> or ARRAY<...>
                    if (!schemaUpper.startsWith("ROW<") && !schemaUpper.startsWith("MAP<") && !schemaUpper.startsWith("ARRAY<")) {
                        throw new IllegalArgumentException(
                                "schema parameter must be either ROW<...>, MAP<...>, or ARRAY<...>. - got: "
                                        + schemaString);
                    }
                    // Pass the original schemaString to preserve field name casing
                    DataType outputType = SchemaParser.parseType(schemaString);
                    return Optional.of(outputType);
                })
                .build();
    }
}
