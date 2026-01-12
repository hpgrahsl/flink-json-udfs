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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void testSimpleSchema() {
        DataType dataType = SchemaParser.parseSchema("id INT, name STRING");

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(2, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
        assertTrue(rowType.getTypeAt(1) instanceof VarCharType);
    }

    @Test
    void testAllBasicTypes() {
        String schema = "a TINYINT, b SMALLINT, c INT, d BIGINT, e FLOAT, f DOUBLE, " +
                       "g STRING, h BOOLEAN, i BINARY";
        DataType dataType = SchemaParser.parseSchema(schema);
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(9, rowType.getFieldCount());
        assertTrue(rowType.getTypeAt(0) instanceof TinyIntType);
        assertTrue(rowType.getTypeAt(1) instanceof SmallIntType);
        assertTrue(rowType.getTypeAt(2) instanceof IntType);
        assertTrue(rowType.getTypeAt(3) instanceof BigIntType);
        assertTrue(rowType.getTypeAt(4) instanceof FloatType);
        assertTrue(rowType.getTypeAt(5) instanceof DoubleType);
        assertTrue(rowType.getTypeAt(6) instanceof VarCharType);
        assertTrue(rowType.getTypeAt(7) instanceof BooleanType);
        assertTrue(rowType.getTypeAt(8) instanceof BinaryType);
    }

    @Test
    void testArrayType() {
        DataType dataType = SchemaParser.parseSchema("tags ARRAY<STRING>");
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(1, rowType.getFieldCount());
        assertEquals("tags", rowType.getFieldNames().get(0));
        assertTrue(rowType.getTypeAt(0) instanceof ArrayType);

        ArrayType arrayType = (ArrayType) rowType.getTypeAt(0);
        assertTrue(arrayType.getElementType() instanceof VarCharType);
    }

    @Test
    void testNestedRowType() {
        DataType dataType = SchemaParser.parseSchema("address ROW<city STRING, zip STRING>");
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(1, rowType.getFieldCount());
        assertEquals("address", rowType.getFieldNames().get(0));
        assertTrue(rowType.getTypeAt(0) instanceof RowType);

        RowType nestedRow = (RowType) rowType.getTypeAt(0);
        assertEquals(2, nestedRow.getFieldCount());
        assertEquals("city", nestedRow.getFieldNames().get(0));
        assertEquals("zip", nestedRow.getFieldNames().get(1));
    }

    @Test
    void testDeeplyNestedRowType() {
        String schema = "user ROW<id INT, profile ROW<name STRING, age INT>>";
        DataType dataType = SchemaParser.parseSchema(schema);
        RowType rowType = (RowType) dataType.getLogicalType();

        RowType userRow = (RowType) rowType.getTypeAt(0);
        assertEquals(2, userRow.getFieldCount());

        RowType profileRow = (RowType) userRow.getTypeAt(1);
        assertEquals(2, profileRow.getFieldCount());
        assertEquals("name", profileRow.getFieldNames().get(0));
        assertEquals("age", profileRow.getFieldNames().get(1));
    }

    @Test
    void testArrayOfRows() {
        DataType dataType = SchemaParser.parseSchema("items ARRAY<ROW<id INT, name STRING>>");
        RowType rowType = (RowType) dataType.getLogicalType();

        ArrayType arrayType = (ArrayType) rowType.getTypeAt(0);
        assertTrue(arrayType.getElementType() instanceof RowType);

        RowType elementRow = (RowType) arrayType.getElementType();
        assertEquals(2, elementRow.getFieldCount());
    }

    @Test
    void testCaseInsensitiveTypes() {
        DataType dataType1 = SchemaParser.parseSchema("id int");
        DataType dataType2 = SchemaParser.parseSchema("id INT");
        DataType dataType3 = SchemaParser.parseSchema("id Int");

        RowType rowType1 = (RowType) dataType1.getLogicalType();
        RowType rowType2 = (RowType) dataType2.getLogicalType();
        RowType rowType3 = (RowType) dataType3.getLogicalType();

        assertTrue(rowType1.getTypeAt(0) instanceof IntType);
        assertTrue(rowType2.getTypeAt(0) instanceof IntType);
        assertTrue(rowType3.getTypeAt(0) instanceof IntType);
    }

    @Test
    void testIntegerTypeAlias() {
        // INTEGER should be an alias for INT
        DataType dataTypeInt = SchemaParser.parseSchema("id INT");
        DataType dataTypeInteger = SchemaParser.parseSchema("id INTEGER");
        DataType dataTypeMixed = SchemaParser.parseSchema("count INTEGER, age INT");

        RowType rowTypeInt = (RowType) dataTypeInt.getLogicalType();
        RowType rowTypeInteger = (RowType) dataTypeInteger.getLogicalType();
        RowType rowTypeMixed = (RowType) dataTypeMixed.getLogicalType();

        // Both should produce IntType
        assertTrue(rowTypeInt.getTypeAt(0) instanceof IntType);
        assertTrue(rowTypeInteger.getTypeAt(0) instanceof IntType);

        // Mixed usage should work
        assertEquals(2, rowTypeMixed.getFieldCount());
        assertTrue(rowTypeMixed.getTypeAt(0) instanceof IntType);
        assertTrue(rowTypeMixed.getTypeAt(1) instanceof IntType);
    }

    @Test
    void testWhitespaceHandling() {
        DataType dataType = SchemaParser.parseSchema("  id   INT  ,  name   STRING  ");
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(2, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
    }

    @Test
    void testGetFieldNames() {
        DataType dataType = SchemaParser.parseSchema("id INT, name STRING, active BOOLEAN");
        List<String> fieldNames = SchemaParser.getFieldNames(dataType);

        assertEquals(3, fieldNames.size());
        assertEquals("id", fieldNames.get(0));
        assertEquals("name", fieldNames.get(1));
        assertEquals("active", fieldNames.get(2));
    }

    @Test
    void testNullSchemaThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseSchema(null));
    }

    @Test
    void testEmptySchemaThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseSchema(""));
    }

    @Test
    void testInvalidFieldFormatThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseSchema("invalidfield"));
    }

    @Test
    void testUnsupportedTypeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseSchema("id UNSUPPORTED_TYPE"));
    }


    @Test
    void testComplexMixedSchema() {
        String schema = "id INT, " +
                       "name STRING, " +
                       "tags ARRAY<STRING>, " +
                       "address ROW<city STRING, zip STRING>, " +
                       "active BOOLEAN, " +
                       "score DOUBLE";

        DataType dataType = SchemaParser.parseSchema(schema);
        RowType rowType = (RowType) dataType.getLogicalType();

        assertEquals(6, rowType.getFieldCount());
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
        assertTrue(rowType.getTypeAt(1) instanceof VarCharType);
        assertTrue(rowType.getTypeAt(2) instanceof ArrayType);
        assertTrue(rowType.getTypeAt(3) instanceof RowType);
        assertTrue(rowType.getTypeAt(4) instanceof BooleanType);
        assertTrue(rowType.getTypeAt(5) instanceof DoubleType);
    }

    @Test
    void testMapType() {
        DataType dataType = SchemaParser.parseType("MAP<STRING, INT>");

        assertTrue(dataType.getLogicalType() instanceof MapType);
        MapType mapType =
            (MapType) dataType.getLogicalType();

        assertTrue(mapType.getKeyType() instanceof VarCharType);
        assertTrue(mapType.getValueType() instanceof IntType);
    }

    @Test
    void testMapWithComplexValueType() {
        DataType dataType = SchemaParser.parseType("MAP<STRING, ROW<id INT, name STRING>>");

        assertTrue(dataType.getLogicalType() instanceof MapType);
        MapType mapType =
            (MapType) dataType.getLogicalType();

        assertTrue(mapType.getKeyType() instanceof VarCharType);
        assertTrue(mapType.getValueType() instanceof RowType);

        RowType valueRowType = (RowType) mapType.getValueType();
        assertEquals(2, valueRowType.getFieldCount());
        assertEquals("id", valueRowType.getFieldNames().get(0));
        assertEquals("name", valueRowType.getFieldNames().get(1));
    }

    @Test
    void testMapWithArrayValueType() {
        DataType dataType = SchemaParser.parseType("MAP<STRING, ARRAY<INT>>");

        assertTrue(dataType.getLogicalType() instanceof MapType);
        MapType mapType =
            (MapType) dataType.getLogicalType();

        assertTrue(mapType.getKeyType() instanceof VarCharType);
        assertTrue(mapType.getValueType() instanceof ArrayType);
    }

    @Test
    void testInvalidMapFormatThrowsException() {
        // MAP needs exactly 2 type parameters
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseType("MAP<STRING>"));
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parseType("MAP<STRING, INT, BOOLEAN>"));
    }

    @Test
    void testParseTypeSimpleArray() {
        DataType dataType = SchemaParser.parseType("ARRAY<INT>");

        assertTrue(dataType.getLogicalType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType.getLogicalType();
        assertTrue(arrayType.getElementType() instanceof IntType);
    }

    @Test
    void testParseTypeSimpleRow() {
        DataType dataType = SchemaParser.parseType("ROW<id INT, name STRING>");

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType rowType = (RowType) dataType.getLogicalType();
        assertEquals(2, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
        assertTrue(rowType.getTypeAt(1) instanceof VarCharType);
    }

    @Test
    void testParseTypeNestedArrayInRow() {
        DataType dataType = SchemaParser.parseType("ROW<id INT, tags ARRAY<STRING>>");

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType rowType = (RowType) dataType.getLogicalType();
        assertEquals(2, rowType.getFieldCount());
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
        assertTrue(rowType.getTypeAt(1) instanceof ArrayType);

        ArrayType tagsArray = (ArrayType) rowType.getTypeAt(1);
        assertTrue(tagsArray.getElementType() instanceof VarCharType);
    }

    @Test
    void testParseTypeNestedRowInArray() {
        DataType dataType = SchemaParser.parseType("ARRAY<ROW<id INT, name STRING>>");

        assertTrue(dataType.getLogicalType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType.getLogicalType();
        assertTrue(arrayType.getElementType() instanceof RowType);

        RowType elementRowType = (RowType) arrayType.getElementType();
        assertEquals(2, elementRowType.getFieldCount());
        assertEquals("id", elementRowType.getFieldNames().get(0));
        assertEquals("name", elementRowType.getFieldNames().get(1));
    }

    @Test
    void testParseTypeDeeplyNestedStructure() {
        // ROW containing ROW containing ARRAY
        DataType dataType = SchemaParser.parseType(
            "ROW<user ROW<id INT, profile ROW<name STRING, tags ARRAY<STRING>>>>"
        );

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType outerRow = (RowType) dataType.getLogicalType();
        assertEquals(1, outerRow.getFieldCount());
        assertEquals("user", outerRow.getFieldNames().get(0));

        // First level nesting: user field
        assertTrue(outerRow.getTypeAt(0) instanceof RowType);
        RowType userRow = (RowType) outerRow.getTypeAt(0);
        assertEquals(2, userRow.getFieldCount());
        assertEquals("id", userRow.getFieldNames().get(0));
        assertEquals("profile", userRow.getFieldNames().get(1));
        assertTrue(userRow.getTypeAt(0) instanceof IntType);

        // Second level nesting: profile field
        assertTrue(userRow.getTypeAt(1) instanceof RowType);
        RowType profileRow = (RowType) userRow.getTypeAt(1);
        assertEquals(2, profileRow.getFieldCount());
        assertEquals("name", profileRow.getFieldNames().get(0));
        assertEquals("tags", profileRow.getFieldNames().get(1));
        assertTrue(profileRow.getTypeAt(0) instanceof VarCharType);

        // Third level nesting: tags array
        assertTrue(profileRow.getTypeAt(1) instanceof ArrayType);
        ArrayType tagsArray = (ArrayType) profileRow.getTypeAt(1);
        assertTrue(tagsArray.getElementType() instanceof VarCharType);
    }

    @Test
    void testParseTypeArrayOfArrays() {
        DataType dataType = SchemaParser.parseType("ARRAY<ARRAY<INT>>");

        assertTrue(dataType.getLogicalType() instanceof ArrayType);
        ArrayType outerArray = (ArrayType) dataType.getLogicalType();
        assertTrue(outerArray.getElementType() instanceof ArrayType);

        ArrayType innerArray = (ArrayType) outerArray.getElementType();
        assertTrue(innerArray.getElementType() instanceof IntType);
    }

    @Test
    void testParseTypeRowWithMultipleComplexFields() {
        DataType dataType = SchemaParser.parseType(
            "ROW<id INT, tags ARRAY<STRING>, metadata MAP<STRING, INT>, address ROW<city STRING, zip STRING>>"
        );

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType rowType = (RowType) dataType.getLogicalType();
        assertEquals(4, rowType.getFieldCount());

        // Field 0: id INT
        assertEquals("id", rowType.getFieldNames().get(0));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);

        // Field 1: tags ARRAY<STRING>
        assertEquals("tags", rowType.getFieldNames().get(1));
        assertTrue(rowType.getTypeAt(1) instanceof ArrayType);
        ArrayType tagsArray = (ArrayType) rowType.getTypeAt(1);
        assertTrue(tagsArray.getElementType() instanceof VarCharType);

        // Field 2: metadata MAP<STRING, INT>
        assertEquals("metadata", rowType.getFieldNames().get(2));
        assertTrue(rowType.getTypeAt(2) instanceof MapType);
        MapType metadataMap =
            (MapType) rowType.getTypeAt(2);
        assertTrue(metadataMap.getKeyType() instanceof VarCharType);
        assertTrue(metadataMap.getValueType() instanceof IntType);

        // Field 3: address ROW<city STRING, zip STRING>
        assertEquals("address", rowType.getFieldNames().get(3));
        assertTrue(rowType.getTypeAt(3) instanceof RowType);
        RowType addressRow = (RowType) rowType.getTypeAt(3);
        assertEquals(2, addressRow.getFieldCount());
        assertEquals("city", addressRow.getFieldNames().get(0));
        assertEquals("zip", addressRow.getFieldNames().get(1));
    }

    @Test
    void testParseTypeArrayOfMaps() {
        DataType dataType = SchemaParser.parseType("ARRAY<MAP<STRING, INT>>");

        assertTrue(dataType.getLogicalType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType.getLogicalType();
        assertTrue(arrayType.getElementType() instanceof MapType);

        MapType mapType =
            (MapType) arrayType.getElementType();
        assertTrue(mapType.getKeyType() instanceof VarCharType);
        assertTrue(mapType.getValueType() instanceof IntType);
    }

    @Test
    void testParseTypeMapWithNestedArrayValue() {
        DataType dataType = SchemaParser.parseType("MAP<STRING, ARRAY<ROW<id INT, value DOUBLE>>>");

        assertTrue(dataType.getLogicalType() instanceof MapType);
        MapType mapType =
            (MapType) dataType.getLogicalType();

        assertTrue(mapType.getKeyType() instanceof VarCharType);
        assertTrue(mapType.getValueType() instanceof ArrayType);

        ArrayType arrayType = (ArrayType) mapType.getValueType();
        assertTrue(arrayType.getElementType() instanceof RowType);

        RowType rowType = (RowType) arrayType.getElementType();
        assertEquals(2, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("value", rowType.getFieldNames().get(1));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
        assertTrue(rowType.getTypeAt(1) instanceof DoubleType);
    }

    @Test
    void testParseTypeAllPrimitiveTypes() {
        String schema = "ROW<" +
            "a TINYINT, b SMALLINT, c INT, d BIGINT, " +
            "e FLOAT, f DOUBLE, g DECIMAL, " +
            "h STRING, i BOOLEAN, j BINARY, " +
            "k DATE, l TIME, m TIMESTAMP" +
            ">";

        DataType dataType = SchemaParser.parseType(schema);
        assertTrue(dataType.getLogicalType() instanceof RowType);

        RowType rowType = (RowType) dataType.getLogicalType();
        assertEquals(13, rowType.getFieldCount());

        assertTrue(rowType.getTypeAt(0) instanceof TinyIntType);
        assertTrue(rowType.getTypeAt(1) instanceof SmallIntType);
        assertTrue(rowType.getTypeAt(2) instanceof IntType);
        assertTrue(rowType.getTypeAt(3) instanceof BigIntType);
        assertTrue(rowType.getTypeAt(4) instanceof FloatType);
        assertTrue(rowType.getTypeAt(5) instanceof DoubleType);
        assertTrue(rowType.getTypeAt(6) instanceof DecimalType);
        assertTrue(rowType.getTypeAt(7) instanceof VarCharType);
        assertTrue(rowType.getTypeAt(8) instanceof BooleanType);
        assertTrue(rowType.getTypeAt(9) instanceof BinaryType);
        assertTrue(rowType.getTypeAt(10) instanceof DateType);
        assertTrue(rowType.getTypeAt(11) instanceof TimeType);
        // TIMESTAMP without time zone maps to TimestampType (LocalDateTime)
        assertTrue(rowType.getTypeAt(12) instanceof TimestampType);
    }

    @Test
    void testParameterizedVarchar() {
        DataType dataType = SchemaParser.parseType("VARCHAR(50)");

        assertTrue(dataType.getLogicalType() instanceof VarCharType);
        VarCharType varCharType = (VarCharType) dataType.getLogicalType();
        assertEquals(50, varCharType.getLength());
    }

    @Test
    void testParameterizedChar() {
        DataType dataType = SchemaParser.parseType("CHAR(10)");

        assertTrue(dataType.getLogicalType() instanceof CharType);
        CharType charType =
            (CharType) dataType.getLogicalType();
        assertEquals(10, charType.getLength());
    }

    @Test
    void testParameterizedDecimal() {
        DataType dataType = SchemaParser.parseType("DECIMAL(10, 2)");

        assertTrue(dataType.getLogicalType() instanceof DecimalType);
        DecimalType decimalType =
            (DecimalType) dataType.getLogicalType();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    void testParameterizedDecimalPrecisionOnly() {
        DataType dataType = SchemaParser.parseType("DECIMAL(15)");

        assertTrue(dataType.getLogicalType() instanceof DecimalType);
        DecimalType decimalType =
            (DecimalType) dataType.getLogicalType();
        assertEquals(15, decimalType.getPrecision());
        assertEquals(0, decimalType.getScale());
    }

    @Test
    void testParameterizedVarbinary() {
        DataType dataType = SchemaParser.parseType("VARBINARY(100)");

        assertTrue(dataType.getLogicalType() instanceof VarBinaryType);
        VarBinaryType varbinaryType = (VarBinaryType) dataType.getLogicalType();
        assertEquals(100, varbinaryType.getLength());
    }

    @Test
    void testParameterizedTime() {
        DataType dataType = SchemaParser.parseType("TIME(3)");

        assertTrue(dataType.getLogicalType() instanceof TimeType);
        TimeType timeType =
            (TimeType) dataType.getLogicalType();
        assertEquals(3, timeType.getPrecision());
    }

    @Test
    void testParameterizedTimestamp() {
        DataType dataType = SchemaParser.parseType("TIMESTAMP(6)");

        assertTrue(dataType.getLogicalType() instanceof TimestampType);
        TimestampType timestampType =
            (TimestampType) dataType.getLogicalType();
        assertEquals(6, timestampType.getPrecision());
    }

    @Test
    void testParameterizedTypesInRow() {
        DataType dataType = SchemaParser.parseType(
            "ROW<name VARCHAR(100), code CHAR(5), price DECIMAL(10,2)>"
        );

        assertTrue(dataType.getLogicalType() instanceof RowType);
        RowType rowType = (RowType) dataType.getLogicalType();
        assertEquals(3, rowType.getFieldCount());

        // name VARCHAR(100)
        VarCharType nameType = (VarCharType) rowType.getTypeAt(0);
        assertEquals(100, nameType.getLength());

        // code CHAR(5)
        CharType codeType =
            (CharType) rowType.getTypeAt(1);
        assertEquals(5, codeType.getLength());

        // price DECIMAL(10,2)
        DecimalType priceType =
            (DecimalType) rowType.getTypeAt(2);
        assertEquals(10, priceType.getPrecision());
        assertEquals(2, priceType.getScale());
    }

    @Test
    void testParameterizedTypesInNestedStructure() {
        DataType dataType = SchemaParser.parseType(
            "ARRAY<ROW<id INT, description VARCHAR(200), metadata MAP<STRING, VARCHAR(50)>>>"
        );

        assertTrue(dataType.getLogicalType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType.getLogicalType();

        RowType rowType = (RowType) arrayType.getElementType();
        assertEquals(3, rowType.getFieldCount());

        // description VARCHAR(200)
        VarCharType descType = (VarCharType) rowType.getTypeAt(1);
        assertEquals(200, descType.getLength());

        // metadata MAP<STRING, VARCHAR(50)>
        MapType mapType =
            (MapType) rowType.getTypeAt(2);
        VarCharType mapValueType = (VarCharType) mapType.getValueType();
        assertEquals(50, mapValueType.getLength());
    }
}
