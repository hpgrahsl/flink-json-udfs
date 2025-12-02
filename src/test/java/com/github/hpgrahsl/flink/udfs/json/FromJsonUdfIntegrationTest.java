package com.github.hpgrahsl.flink.udfs.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class FromJsonUdfIntegrationTest {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(2)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    static StreamExecutionEnvironment ENV;
    static StreamTableEnvironment T_ENV;

    @BeforeAll
    static void setUp(@InjectMiniCluster MiniCluster miniCluster) {
        ENV = StreamExecutionEnvironment.getExecutionEnvironment();
        ENV.setParallelism(2);
        T_ENV = StreamTableEnvironment.create(ENV, EnvironmentSettings.newInstance().inStreamingMode().build());
        T_ENV.createTemporaryFunction("FROM_JSON", FromJsonUdf.class);
    }

    @AfterEach
    void cleanUpTemporaryResources() {
        T_ENV.dropTemporaryView("input_table");
    }

    @Test
    public void testArrayOfRowsWithPrimitiveTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("[{\"id\": 1, \"name\": \"Alice\", \"age\": 30}, {\"id\": 2, \"name\": \"Bob\", \"age\": 25}]")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ARRAY<ROW<id INT, name STRING, age INT>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row[]> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row[] rows = results.get(0);
        assertEquals(2, rows.length);
        assertEquals(Row.of(1, "Alice", 30), rows[0]);
        assertEquals(Row.of(2, "Bob", 25), rows[1]);
    }

    @Test
    public void testSingleJsonObject() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 42, \"name\": \"Charlie\"}"),
            Row.of("{\"id\": 99, \"name\": \"Dave\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, name STRING>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertThat(results, containsInAnyOrder(
            Row.of(42, "Charlie"),
            Row.of(99, "Dave")
        ));
    }

    @Test
    public void testArrayOfPrimitives() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("[1, 2, 3, 4, 5]"),
            Row.of("[10, 20, 30]")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ARRAY<INT>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Integer[]> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(2, results.size());

        // Verify array [1, 2, 3, 4, 5] exists with all elements
        assertTrue(results.stream().anyMatch(arr -> {
            try {
                assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, arr);
                return true;
            } catch (AssertionError e) {
                return false;
            }
        }), "Expected to find array [1, 2, 3, 4, 5]");

        // Verify array [10, 20, 30] exists with all elements
        assertTrue(results.stream().anyMatch(arr -> {
            try {
                assertArrayEquals(new Integer[]{10, 20, 30}, arr);
                return true;
            } catch (AssertionError e) {
                return false;
            }
        }), "Expected to find array [10, 20, 30]");
    }

    @Test
    public void testNestedArrays() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("[[1, 2], [3, 4, 5]]")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ARRAY<ARRAY<INT>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Integer[][]> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Integer[][] nested = results.get(0);
        assertEquals(2, nested.length);
        assertArrayEquals(new Integer[]{1, 2}, nested[0]);
        assertArrayEquals(new Integer[]{3, 4, 5}, nested[1]);
    }

    @Test
    public void testRowWithNestedRow() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 1, \"address\": {\"city\": \"NYC\", \"zip\": 10001}}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, address ROW<city STRING, zip INT>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(1, result.getField(0));
        Row address = (Row) result.getField(1);
        assertEquals("NYC", address.getField(0));
        assertEquals(10001, address.getField(1));
    }

    @Test
    public void testRowWithArray() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 1, \"tags\": [\"java\", \"flink\", \"sql\"]}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, tags ARRAY<STRING>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(1, result.getField(0));
        String[] tags = (String[]) result.getField(1);
        assertArrayEquals(new String[]{"java", "flink", "sql"}, tags);
    }

    @Test
    public void testMapType() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"color\": \"red\", \"size\": \"large\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'MAP<STRING, STRING>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Map<Object, Object>> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Map<Object, Object> map = results.get(0);
        assertEquals(2, map.size());
        assertEquals("red", map.get("color"));
        assertEquals("large", map.get("size"));
    }

    @Test
    public void testRowWithMap() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 1, \"metadata\": {\"version\": \"1.0\", \"author\": \"Alice\"}}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, metadata MAP<STRING, STRING>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(1, result.getField(0));
        @SuppressWarnings("unchecked")
        Map<Object, Object> metadata = (Map<Object, Object>) result.getField(1);
        assertEquals("1.0", metadata.get("version"));
        assertEquals("Alice", metadata.get("author"));
    }

    @Test
    public void testDateTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"birthdate\": \"1990-05-15\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<birthdate DATE>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(LocalDate.of(1990, 5, 15), result.getField(0));
    }

    @Test
    public void testTimeTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"start_time\": \"09:30:00\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<start_time TIME>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(LocalTime.of(9, 30, 0), result.getField(0));
    }

    @Test
    public void testTimestampTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"created_at\": \"2024-01-15T10:30:00\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<created_at TIMESTAMP>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30, 0), result.getField(0));
    }

    @Test
    public void testTimestampLtzTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"event_time\": \"2024-01-15T10:30:00Z\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<event_time TIMESTAMP_LTZ>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(Instant.parse("2024-01-15T10:30:00Z"), result.getField(0));
    }

    @Test
    public void testNumericTypes() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"tiny\": 1, \"small\": 100, \"normal\": 1000, \"big\": 1000000, \"float_val\": 3.14, \"double_val\": 2.718}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<tiny TINYINT, small SMALLINT, normal INT, big BIGINT, float_val FLOAT, double_val DOUBLE>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals((byte) 1, result.getField(0));
        assertEquals((short) 100, result.getField(1));
        assertEquals(1000, result.getField(2));
        assertEquals(1000000L, result.getField(3));
        assertEquals(3.14f, (Float) result.getField(4), 0.001);
        assertEquals(2.718, (Double) result.getField(5), 0.001);
    }

    @Test
    public void testEmptyArray() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("[]")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ARRAY<INT>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Integer[]> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        assertEquals(0, results.get(0).length);
    }

    @Test
    public void testNullValues() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 1, \"name\": null, \"age\": 30}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, name STRING, age INT>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(1, result.getField(0));
        assertNull(result.getField(1));
        assertEquals(30, result.getField(2));
    }

    @Test
    public void testBooleanType() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"active\": true, \"verified\": false}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<active BOOLEAN, verified BOOLEAN>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(true, result.getField(0));
        assertEquals(false, result.getField(1));
    }

    @Test
    public void testParameterizedVarchar() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"short_text\": \"hello\"}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<short_text VARCHAR(50)>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals("hello", result.getField(0));
    }

    @Test
    public void testComplexNestedStructure() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"user_id\": 123, \"profile\": {\"name\": \"Alice\", \"emails\": [\"alice@example.com\", \"alice@work.com\"]}, \"settings\": {\"theme\": \"dark\", \"notifications\": \"enabled\"}}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<user_id INT, profile ROW<name STRING, emails ARRAY<STRING>>, settings MAP<STRING, STRING>>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(123, result.getField(0));

        Row profile = (Row) result.getField(1);
        assertEquals("Alice", profile.getField(0));
        String[] emails = (String[]) profile.getField(1);
        assertArrayEquals(new String[]{"alice@example.com", "alice@work.com"}, emails);

        @SuppressWarnings("unchecked")
        Map<Object, Object> settings = (Map<Object, Object>) result.getField(2);
        assertEquals("dark", settings.get("theme"));
        assertEquals("enabled", settings.get("notifications"));
    }

    @Test
    public void testMultipleRowsWithDifferentData() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(DataTypes.FIELD("json", DataTypes.STRING())),
            Row.of("{\"id\": 1, \"value\": 100}"),
            Row.of("{\"id\": 2, \"value\": 200}"),
            Row.of("{\"id\": 3, \"value\": 300}")
        );

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
            "SELECT FROM_JSON(json, 'ROW<id INT, value INT>') AS udf_result FROM input_table"
        );
        var outputStream = T_ENV.toDataStream(outputTable);

        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> rowIter = outputStream.executeAndCollect()) {
            rowIter.forEachRemaining(r -> results.add(r.getFieldAs("udf_result")));
        }

        assertEquals(3, results.size());
        assertThat(results, containsInAnyOrder(
            Row.of(1, 100),
            Row.of(2, 200),
            Row.of(3, 300)
        ));
    }
}
