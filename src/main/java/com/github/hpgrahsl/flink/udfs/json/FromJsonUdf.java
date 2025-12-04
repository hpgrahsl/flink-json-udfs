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
     * Parses JSON into properly typed flink data structures based on the given JSON
     * string
     * and the specified target schema. Supports both individual JSON objects and
     * JSON arrays.
     *
     * @param jsonString   JSON string to parse (object or array)
     * @param schemaString Schema definition:
     *                     - For JSON arrays: "ARRAY<element_type>" or
     *                     "ARRAY<ROW<field1 TYPE1, ...>>"
     *                     - For JSON objects: "ROW<field1 TYPE1, field2 TYPE2,
     *                     ...>"
     * @return Row for single object, or Array for JSON array. Details see
     *         {@link #getTypeInference(DataTypeFactory)}
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
     * Alternative method to allow for proper initialization whenever this scalar function gets
     * used in a Flink CDC pipeline context. In this case, lifecycle hooks such as the
     * open(...) method won't get automatically called unfortunately.
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
     * Gets a cached parsed schema or parses and caches it if not present.
     * Uses LRU eviction when cache is full.
     */
    private DataType getCachedSchema(String schemaString) {
        return schemaCache.computeIfAbsent(schemaString, SchemaParser::parseType);
    }

    /**
     * Parses a single JSON object into a Row.
     * Schema format: "ROW<field1 TYPE1, field2 TYPE2, ...>"
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
     * Parses a JSON object into a Map.
     * Schema format: "MAP<KEY_TYPE, VALUE_TYPE>"
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
     * Parses a JSON array into an Object array.
     * Schema format: "ARRAY<element_type>" where element_type can be:
     * - A simple type: "ARRAY<INT>", "ARRAY<STRING>"
     * - A ROW type: "ARRAY<ROW<field1 TYPE1, field2 TYPE2>>"
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
     * Converts a JsonNode to a Java object based on the target Flink type.
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
     * Converts a string (JSON object key) to the target type.
     * JSON object keys are always strings, but MAP keys can be of any type.
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

    private Object createTypedArray(List<Object> elements, LogicalType elementType) {
        Class<?> elementClass = getDefaultConversionClass(elementType);
        Object typedArray = Array.newInstance(elementClass, elements.size());
        for (int i = 0; i < elements.size(); i++) {
            Array.set(typedArray, i, elements.get(i));
        }
        return typedArray;
    }

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

    private LocalTime parseLocalTime(String value) {
        try {
            return LocalTime.parse(value);
        } catch (DateTimeParseException e) {
            return java.sql.Time.valueOf(value).toLocalTime();
        }
    }

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
                    String schema = schemaString.trim().toUpperCase();
                    // schema must be one of ROW<...> or MAP<...> or ARRAY<...>
                    if (!schema.startsWith("ROW<") && !schema.startsWith("MAP<") && !schema.startsWith("ARRAY<")) {
                        throw new IllegalArgumentException(
                                "schema parameter must be either ROW<...>, MAP<...>, or ARRAY<...>. - got: "
                                        + schemaString);
                    }
                    DataType outputType = SchemaParser.parseType(schema);
                    return Optional.of(outputType);
                })
                .build();
    }
}
