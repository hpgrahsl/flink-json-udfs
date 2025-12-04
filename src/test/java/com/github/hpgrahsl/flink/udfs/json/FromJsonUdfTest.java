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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FromJsonUdfTest {

    private FromJsonUdf function;

    @BeforeEach
    void setUp() throws Exception {
        function = new FromJsonUdf();
        function.open(null);
    }

    @Test
    void testSimpleJsonArray() {
        String json = "[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}]";
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(2, result.length);
        Row row0 = (Row) result[0];
        Row row1 = (Row) result[1];
        assertEquals(1, row0.getField(0));
        assertEquals("Alice", row0.getField(1));
        assertEquals(2, row1.getField(0));
        assertEquals("Bob", row1.getField(1));
    }

    @Test
    void testSingleJsonObject() {
        String json = "{\"id\": 1, \"name\": \"Alice\"}";
        String schema = "ROW<id INT, name STRING>";
        Row result = (Row) function.eval(json, schema);

        assertEquals(1, result.getField(0));
        assertEquals("Alice", result.getField(1));
    }

    @Test
    void testMixedTypes() {
        String json = "[{\"id\": 100, \"name\": \"Test\", \"active\": true, \"score\": 95.5}]";
        String schema = "ARRAY<ROW<id INT, name STRING, active BOOLEAN, score DOUBLE>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(100, ((Row) result[0]).getField(0));
        assertEquals("Test", ((Row) result[0]).getField(1));
        assertEquals(true, ((Row) result[0]).getField(2));
        assertEquals(95.5, ((Row) result[0]).getField(3));
    }

    @Test
    void testEmptyArray() {
        String json = "[]";
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(0, result.length);
    }

    @Test
    void testNullValues() {
        String json = "[{\"id\": 1, \"name\": null}]";
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(1, ((Row) result[0]).getField(0));
        assertNull(((Row) result[0]).getField(1));
    }

    @Test
    void testMissingFields() {
        // Second object missing 'name' field
        String json = "[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2}]";
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(2, result.length);
        assertEquals(1, ((Row) result[0]).getField(0));
        assertEquals("Alice", ((Row) result[0]).getField(1));
        assertEquals(2, ((Row) result[1]).getField(0));
        assertNull(((Row) result[1]).getField(1));
    }

    @Test
    void testNestedObject() {
        String json = "[{\"id\": 1, \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}]";
        String schema = "ARRAY<ROW<id INT, address ROW<city STRING, zip STRING>>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(1, ((Row) result[0]).getField(0));

        Row nestedRow = (Row) ((Row) result[0]).getField(1);
        assertNotNull(nestedRow);
        assertEquals("NYC", nestedRow.getField(0));
        assertEquals("10001", nestedRow.getField(1));
    }

    @Test
    void testNestedArray() {
        String json = "[{\"id\": 1, \"tags\": [\"a\", \"b\", \"c\"]}]";
        String schema = "ARRAY<ROW<id INT, tags ARRAY<STRING>>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(1, ((Row) result[0]).getField(0));

        Object[] tags = (Object[]) ((Row) result[0]).getField(1);
        assertNotNull(tags);
        assertEquals(3, tags.length);
        assertEquals("a", tags[0]);
        assertEquals("b", tags[1]);
        assertEquals("c", tags[2]);
    }

    @Test
    void testLongNumbers() {
        String json = "[{\"id\": 9223372036854775807}]";
        String schema = "ARRAY<ROW<id BIGINT>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(9223372036854775807L, ((Row) result[0]).getField(0));
    }

    @Test
    void testInvalidJsonThrowsException() {
        String invalidJson = "{invalid json}";
        String schema = "id INT, name STRING";
        assertThrows(RuntimeException.class, () -> function.eval(invalidJson, schema));
    }

    @Test
    void testSingleJsonObjectReturnsRow() {
        // Single JSON objects now return a ROW, not an error
        String json = "{\"id\": 1, \"name\": \"Alice\"}";
        String schema = "ROW<id INT, name STRING>";
        Row result = (Row) function.eval(json, schema);
        assertEquals(1, result.getField(0));
        assertEquals("Alice", result.getField(1));
    }

    @Test
    void testArrayOfPrimitives() {
        // Arrays of primitives should work when schema is a simple type
        String json = "[1, 2, 3]";
        String schema = "ARRAY<INT>";
        Object[] result = (Object[]) function.eval(json, schema);
        assertEquals(3, result.length);
        assertEquals(1, result[0]);
        assertEquals(2, result[1]);
        assertEquals(3, result[2]);
    }

    @Test
    void testComplexNestedStructure() {
        String json = "[{\"user\": {\"id\": 1, \"profile\": {\"name\": \"Alice\", \"age\": 30}}, \"active\": true}]";
        String schema = "ARRAY<ROW<user ROW<id INT, profile ROW<name STRING, age INT>>, active BOOLEAN>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);

        Row userRow = (Row) ((Row) result[0]).getField(0);
        assertNotNull(userRow);
        assertEquals(1, userRow.getField(0));

        Row profileRow = (Row) userRow.getField(1);
        assertNotNull(profileRow);
        assertEquals("Alice", profileRow.getField(0));
        assertEquals(30, profileRow.getField(1));

        assertEquals(true, ((Row) result[0]).getField(1));
    }

    @Test
    void testFieldOrderPreservation() {
        String json = "[{\"z\": 3, \"a\": 1, \"m\": 2}]";
        String schema = "ARRAY<ROW<z INT, a INT, m INT>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        // Fields should match schema order
        assertEquals(3, ((Row) result[0]).getField(0));
        assertEquals(1, ((Row) result[0]).getField(1));
        assertEquals(2, ((Row) result[0]).getField(2));
    }

    @Test
    void testNullSchemaThrowsException() {
        String json = "[{\"id\": 1}]";
        assertThrows(IllegalArgumentException.class, () -> function.eval(json, null));
    }

    @Test
    void testEmptySchemaThrowsException() {
        String json = "[{\"id\": 1}]";
        assertThrows(IllegalArgumentException.class, () -> function.eval(json, ""));
    }

    @Test
    void testInvalidSchemaThrowsException() {
        String json = "[{\"id\": 1}]";
        assertThrows(IllegalArgumentException.class, () -> function.eval(json, "invalid schema"));
    }

    @Test
    void testExtraFieldsInJsonIgnored() {
        String json = "[{\"id\": 1, \"name\": \"Alice\", \"extra\": \"ignored\"}]";
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals(2, ((Row) result[0]).getArity()); // Only 2 fields
        assertEquals(1, ((Row) result[0]).getField(0));
        assertEquals("Alice", ((Row) result[0]).getField(1));
    }

    @Test
    void testAllNumericTypes() {
        String json = "[{\"b\": 1, \"s\": 100, \"i\": 1000, \"l\": 10000, \"f\": 1.5, \"d\": 2.5}]";
        String schema = "ARRAY<ROW<b TINYINT, s SMALLINT, i INT, l BIGINT, f FLOAT, d DOUBLE>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        assertEquals((byte) 1, ((Row) result[0]).getField(0));
        assertEquals((short) 100, ((Row) result[0]).getField(1));
        assertEquals(1000, ((Row) result[0]).getField(2));
        assertEquals(10000L, ((Row) result[0]).getField(3));
        assertEquals(1.5f, ((Row) result[0]).getField(4));
        assertEquals(2.5, ((Row) result[0]).getField(5));
    }

    @Test
    void testSimpleMap() {
        String json = "{\"key1\": 100, \"key2\": 200, \"key3\": 300}";
        String schema = "MAP<STRING, INT>";
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) function.eval(json, schema);

        assertEquals(3, result.size());
        assertEquals(100, result.get("key1"));
        assertEquals(200, result.get("key2"));
        assertEquals(300, result.get("key3"));
    }

    @Test
    void testMapWithComplexValues() {
        String json = "{\"user1\": {\"id\": 1, \"name\": \"Alice\"}, \"user2\": {\"id\": 2, \"name\": \"Bob\"}}";
        String schema = "MAP<STRING, ROW<id INT, name STRING>>";
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) function.eval(json, schema);

        assertEquals(2, result.size());

        Row user1 = (Row) result.get("user1");
        assertEquals(1, user1.getField(0));
        assertEquals("Alice", user1.getField(1));

        Row user2 = (Row) result.get("user2");
        assertEquals(2, user2.getField(0));
        assertEquals("Bob", user2.getField(1));
    }

    @Test
    void testMapWithArrayValues() {
        String json = "{\"fruits\": [\"apple\", \"banana\"], \"veggies\": [\"carrot\", \"broccoli\"]}";
        String schema = "MAP<STRING, ARRAY<STRING>>";
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) function.eval(json, schema);

        assertEquals(2, result.size());

        Object[] fruits = (Object[]) result.get("fruits");
        assertEquals(2, fruits.length);
        assertEquals("apple", fruits[0]);
        assertEquals("banana", fruits[1]);

        Object[] veggies = (Object[]) result.get("veggies");
        assertEquals(2, veggies.length);
        assertEquals("carrot", veggies[0]);
        assertEquals("broccoli", veggies[1]);
    }

    @Test
    void testMapWithIntegerKeys() {
        // JSON keys are strings, but we convert them to INT
        String json = "{\"1\": \"first\", \"2\": \"second\", \"3\": \"third\"}";
        String schema = "MAP<INT, STRING>";
        @SuppressWarnings("unchecked")
        Map<Object, Object> result = (Map<Object, Object>) function.eval(json, schema);

        assertEquals(3, result.size());
        assertEquals("first", result.get(1));
        assertEquals("second", result.get(2));
        assertEquals("third", result.get(3));
    }

    @Test
    void testMapFieldInRow() {
        String json = "[{\"id\": 1, \"metadata\": {\"color\": \"red\", \"size\": \"large\"}}]";
        String schema = "ARRAY<ROW<id INT, metadata MAP<STRING, STRING>>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(1, result.length);
        Row row = (Row) result[0];
        assertEquals(1, row.getField(0));

        @SuppressWarnings("unchecked")
        Map<Object, Object> metadata = (Map<Object, Object>) row.getField(1);
        assertEquals(2, metadata.size());
        assertEquals("red", metadata.get("color"));
        assertEquals("large", metadata.get("size"));
    }

    @Test
    void testNestedArrayOfIntegers() {
        // Test ARRAY<ARRAY<INT>>
        String json = "[[1, 2, 3], [4, 5], [6, 7, 8, 9]]";
        String schema = "ARRAY<ARRAY<INT>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(3, result.length);

        Object[] innerArray1 = (Object[]) result[0];
        assertEquals(3, innerArray1.length);
        assertEquals(1, innerArray1[0]);
        assertEquals(2, innerArray1[1]);
        assertEquals(3, innerArray1[2]);

        Object[] innerArray2 = (Object[]) result[1];
        assertEquals(2, innerArray2.length);
        assertEquals(4, innerArray2[0]);
        assertEquals(5, innerArray2[1]);

        Object[] innerArray3 = (Object[]) result[2];
        assertEquals(4, innerArray3.length);
        assertEquals(6, innerArray3[0]);
        assertEquals(7, innerArray3[1]);
        assertEquals(8, innerArray3[2]);
        assertEquals(9, innerArray3[3]);
    }

    @Test
    void testNestedArrayOfStrings() {
        // Test ARRAY<ARRAY<STRING>>
        String json = "[[\"a\", \"b\"], [\"c\", \"d\", \"e\"], [\"f\"]]";
        String schema = "ARRAY<ARRAY<STRING>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(3, result.length);

        Object[] innerArray1 = (Object[]) result[0];
        assertEquals(2, innerArray1.length);
        assertEquals("a", innerArray1[0]);
        assertEquals("b", innerArray1[1]);

        Object[] innerArray2 = (Object[]) result[1];
        assertEquals(3, innerArray2.length);
        assertEquals("c", innerArray2[0]);
        assertEquals("d", innerArray2[1]);
        assertEquals("e", innerArray2[2]);

        Object[] innerArray3 = (Object[]) result[2];
        assertEquals(1, innerArray3.length);
        assertEquals("f", innerArray3[0]);
    }

    @Test
    void testTripleNestedArray() {
        // Test ARRAY<ARRAY<ARRAY<INT>>>
        String json = "[[[1, 2], [3]], [[4, 5, 6]], [[7], [8], [9]]]";
        String schema = "ARRAY<ARRAY<ARRAY<INT>>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(3, result.length);

        // First outer array element
        Object[] level1_0 = (Object[]) result[0];
        assertEquals(2, level1_0.length);
        Object[] level2_0_0 = (Object[]) level1_0[0];
        assertEquals(2, level2_0_0.length);
        assertEquals(1, level2_0_0[0]);
        assertEquals(2, level2_0_0[1]);
        Object[] level2_0_1 = (Object[]) level1_0[1];
        assertEquals(1, level2_0_1.length);
        assertEquals(3, level2_0_1[0]);

        // Second outer array element
        Object[] level1_1 = (Object[]) result[1];
        assertEquals(1, level1_1.length);
        Object[] level2_1_0 = (Object[]) level1_1[0];
        assertEquals(3, level2_1_0.length);
        assertEquals(4, level2_1_0[0]);
        assertEquals(5, level2_1_0[1]);
        assertEquals(6, level2_1_0[2]);

        // Third outer array element
        Object[] level1_2 = (Object[]) result[2];
        assertEquals(3, level1_2.length);
        Object[] level2_2_0 = (Object[]) level1_2[0];
        assertEquals(1, level2_2_0.length);
        assertEquals(7, level2_2_0[0]);
        Object[] level2_2_1 = (Object[]) level1_2[1];
        assertEquals(1, level2_2_1.length);
        assertEquals(8, level2_2_1[0]);
        Object[] level2_2_2 = (Object[]) level1_2[2];
        assertEquals(1, level2_2_2.length);
        assertEquals(9, level2_2_2[0]);
    }

    @Test
    void testNestedArrayOfRows() {
        // Test ARRAY<ARRAY<ROW<id INT, name STRING>>>
        String json = "[[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}], [{\"id\": 3, \"name\": \"Charlie\"}]]";
        String schema = "ARRAY<ARRAY<ROW<id INT, name STRING>>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(2, result.length);

        Object[] innerArray1 = (Object[]) result[0];
        assertEquals(2, innerArray1.length);
        Row row1 = (Row) innerArray1[0];
        assertEquals(1, row1.getField(0));
        assertEquals("Alice", row1.getField(1));
        Row row2 = (Row) innerArray1[1];
        assertEquals(2, row2.getField(0));
        assertEquals("Bob", row2.getField(1));

        Object[] innerArray2 = (Object[]) result[1];
        assertEquals(1, innerArray2.length);
        Row row3 = (Row) innerArray2[0];
        assertEquals(3, row3.getField(0));
        assertEquals("Charlie", row3.getField(1));
    }

    @Test
    void testNestedArrayWithNullElements() {
        // Test nested arrays with null elements
        String json = "[[1, 2], null, [3, null, 4]]";
        String schema = "ARRAY<ARRAY<INT>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(3, result.length);

        Object[] innerArray1 = (Object[]) result[0];
        assertEquals(2, innerArray1.length);
        assertEquals(1, innerArray1[0]);
        assertEquals(2, innerArray1[1]);

        assertNull(result[1]);

        Object[] innerArray3 = (Object[]) result[2];
        assertEquals(3, innerArray3.length);
        assertEquals(3, innerArray3[0]);
        assertNull(innerArray3[1]);
        assertEquals(4, innerArray3[2]);
    }

    @Test
    void testEmptyNestedArrays() {
        // Test empty nested arrays
        String json = "[[], [1, 2], []]";
        String schema = "ARRAY<ARRAY<INT>>";
        Object[] result = (Object[]) function.eval(json, schema);

        assertEquals(3, result.length);

        Object[] innerArray1 = (Object[]) result[0];
        assertEquals(0, innerArray1.length);

        Object[] innerArray2 = (Object[]) result[1];
        assertEquals(2, innerArray2.length);
        assertEquals(1, innerArray2[0]);
        assertEquals(2, innerArray2[1]);

        Object[] innerArray3 = (Object[]) result[2];
        assertEquals(0, innerArray3.length);
    }

    @Test
    void testTypedArrayForPrimitives() {
        Object result = function.eval("[1, 2, 3]", "ARRAY<INT>");
        assertTrue(result instanceof Integer[]);
        Integer[] ints = (Integer[]) result;
        assertEquals(3, ints.length);
        assertEquals(1, ints[0]);
    }

    @Test
    void testTypedArrayForRows() {
        Object result = function.eval("[{\"id\":1}]", "ARRAY<ROW<id INT>>");
        assertTrue(result instanceof Row[]);
        Row[] rows = (Row[]) result;
        assertEquals(1, rows.length);
        assertEquals(1, rows[0].getField(0));
    }

    @Test
    void testTypedNestedArrays() {
        Object result = function.eval("[[1,2], [3]]", "ARRAY<ARRAY<INT>>");
        assertTrue(result instanceof Integer[][]);
        Integer[][] nested = (Integer[][]) result;
        assertEquals(2, nested.length);
        assertArrayEquals(new Integer[]{1, 2}, nested[0]);
    }

    @Test
    void testEmptyArrayUsesTypedComponent() {
        Object result = function.eval("[]", "ARRAY<STRING>");
        assertTrue(result instanceof String[]);
        assertEquals(0, ((String[]) result).length);
    }

    @Test
    void testArrayOfDatesReturnsLocalDates() {
        Object result = function.eval("[\"2024-12-24\",\"2024-12-25\"]", "ARRAY<DATE>");
        assertTrue(result instanceof LocalDate[]);
        LocalDate[] dates = (LocalDate[]) result;
        assertEquals(LocalDate.of(2024, 12, 24), dates[0]);
    }

    @Test
    void testArrayOfTimesReturnsLocalTimes() {
        Object result = function.eval("[\"12:30:00\",\"23:59:59\"]", "ARRAY<TIME>");
        assertTrue(result instanceof LocalTime[]);
        LocalTime[] times = (LocalTime[]) result;
        assertEquals(LocalTime.of(12, 30), times[0]);
    }

    @Test
    void testArrayOfTimestampLtzReturnsInstants() {
        Object result = function.eval(
            "[\"2024-01-01 10:00:00\",\"2024-01-01 11:00:00\"]",
            "ARRAY<TIMESTAMP_LTZ>"
        );
        assertTrue(result instanceof Instant[]);
        Instant[] instants = (Instant[]) result;
        assertEquals(Instant.parse("2024-01-01T10:00:00Z"), instants[0]);
    }

    @Test
    void testSchemaCaching() {
        // Test that repeated calls with the same schema work efficiently
        String schema = "ARRAY<ROW<id INT, name STRING>>";
        String json1 = "[{\"id\": 1, \"name\": \"Alice\"}]";
        String json2 = "[{\"id\": 2, \"name\": \"Bob\"}]";
        String json3 = "[{\"id\": 3, \"name\": \"Charlie\"}]";

        // First call should parse and cache the schema
        Object[] result1 = (Object[]) function.eval(json1, schema);
        assertEquals(1, result1.length);
        assertEquals(1, ((Row) result1[0]).getField(0));
        assertEquals("Alice", ((Row) result1[0]).getField(1));

        // Subsequent calls should hit the cache
        Object[] result2 = (Object[]) function.eval(json2, schema);
        assertEquals(1, result2.length);
        assertEquals(2, ((Row) result2[0]).getField(0));
        assertEquals("Bob", ((Row) result2[0]).getField(1));

        Object[] result3 = (Object[]) function.eval(json3, schema);
        assertEquals(1, result3.length);
        assertEquals(3, ((Row) result3[0]).getField(0));
        assertEquals("Charlie", ((Row) result3[0]).getField(1));
    }

    @Test
    void testMultipleSchemaCaching() {
        // Test that multiple different schemas can be cached
        String schema1 = "ARRAY<INT>";
        String schema2 = "ARRAY<STRING>";
        String schema3 = "ROW<id INT, name STRING>";

        Object result1 = function.eval("[1, 2, 3]", schema1);
        assertTrue(result1 instanceof Integer[]);

        Object result2 = function.eval("[\"a\", \"b\"]", schema2);
        assertTrue(result2 instanceof String[]);

        Object result3 = function.eval("{\"id\": 1, \"name\": \"Test\"}", schema3);
        assertTrue(result3 instanceof Row);

        // Reuse schemas to verify they're cached
        Object result4 = function.eval("[4, 5]", schema1);
        assertTrue(result4 instanceof Integer[]);

        Object result5 = function.eval("[\"c\", \"d\"]", schema2);
        assertTrue(result5 instanceof String[]);
    }
}
