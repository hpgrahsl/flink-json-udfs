# Apache Flink JSON UDFs

This repository provides a custom Flink scalar function `FromJsonUdf` to parse JSON objects or arrays into properly typed values according to the provided target schema.

The implementation focuses on a simple contract: you pass a JSON string and a schema string composed of valid Flink data types. The function returns a typed value (ROW, ARRAY, or MAP) matching the provided schema. It supports the most common Flink data types such as primitives, dates/times, decimals, arrays (including nested arrays), maps, and nested rows.

## Motivation

- **type safety:** JSON is parsed into proper Flink data types (INT, BIGINT, BOOLEAN, DECIMAL, DATE, TIMESTAMP, ARRAY<...>, ROW<...>, MAP<...>) such that downstream operations can work on typed values.
- **flexible schema:** supply arbitrary target schemas based on string literals (e.g. `ROW<id INT, profile ROW<name STRING, emails ARRAY<STRING>>>`)
- **dependency free:** no additional dependencies required other than what's bundled with Flink
- **minimum viable schema parser:** handles nested and complex structures: nested ROWs, nested ARRAYS, MAPs, arrays of rows, arrays of arrays, etc.

## Function contract

- suggested function name when registering: `FROM_JSON`
- arguments:
    1. `json: STRING`
    2. `schema: STRING` (must be a string literal)
- returns: a typed value whose Flink data type is derived from the schema string. Valid top-level schema can be one of: `ROW<...>`, `ARRAY<...>`, `MAP<K,V>`.

The examples below are all written so they can be executed directly in the Flink SQL CLI using based on constant `VALUES(...)` — no prior table definitions required.

## Usage Examples

All examples assume you started the Flink SQL client, have previsouly built the function from sources, added the JAR, and registerd the UDF successfully.

You can directly copy & paste the `SELECT` queries into the Flink SQL CLI:

#### Parse a single JSON object into a ROW

```sql
SELECT
    FROM_JSON(v.json, 'ROW<id INT, name STRING>') AS person
FROM (VALUES
    ROW ('{"id": 1, "name": "Alice"}'),
    ROW ('{"id": 2, "name": "Bob"}')
) AS v (json);
```

Result: a single column `person` of ROW type. To extract fields:

```sql
SELECT person.id, person.name
FROM (
	SELECT
        FROM_JSON(v.json, 'ROW<id INT, name STRING>') AS person
    FROM (VALUES
        ROW ('{"id": 1, "name": "Alice"}'),
        ROW ('{"id": 2, "name": "Bob"}')
    ) AS v(json)
);
```

#### Parse an array of primitives

```sql
SELECT
    FROM_JSON (v.json, 'ARRAY<INT>') AS ints
FROM (VALUES
    ROW ('[1, 2, 3]'),
    ROW ('[9, 8]')
) AS v (json);
```

#### Parse an array of ROWs and UNNEST the result

```sql
SELECT id,name
FROM (VALUES
    ROW ('[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]')
) AS v(json)
CROSS JOIN UNNEST(
    FROM_JSON (v.json, 'ARRAY<ROW<id INT, name STRING>>')
) AS t(id, name);
```

#### Parse nested objects and access nested fields

```sql
-- inner query builds a typed ROW named payload, outer query reads nested fields
SELECT payload.`user`.profile.name,payload.active
FROM (
	SELECT 
        FROM_JSON(v.json,'ROW<user ROW<id INT, profile ROW<name STRING, age INT>>, active BOOLEAN>') AS payload
	FROM (VALUES
        ROW('{"user": {"id": 1, "profile": {"name": "Alice", "age": 30}}, "active": true}'),
        ROW('{"user": {"id": 1, "profile": {"name": "Bob", "age": 42}}, "active": false}')
    ) AS v(json)
);
```

#### Parse a MAP and access values by type-casted key

```sql
SELECT parsed_map[1] AS value_for_key_1
FROM (
	SELECT FROM_JSON(v.json, 'MAP<INT, STRING>') AS parsed_map
	FROM (VALUES 
        ROW('{"1":"first","2":"second"}'),
        ROW('{"1":"erster","1":"zweiter"}')
    ) AS v(json)
);
```

#### Parse arrays of dates/timestamps and other specialized types

```sql
-- ARRAY of DATE -> returns DATE[] (LocalDate[] in the Java UDF)
SELECT FROM_JSON(v.json, 'ARRAY<DATE>') AS dates
FROM (VALUES ROW('["2024-12-24", "2024-12-25"]')) AS v(json);

-- TIMESTAMP_LTZ -> returns instants
SELECT FROM_JSON(v.json, 'ARRAY<TIMESTAMP_LTZ(3)>') AS instants
FROM (VALUES ROW('["2024-01-01 10:00:00", "2024-01-01 11:00:00"]')) AS v(json);
```

#### Complex example: nested arrays of ROWs

```sql
SELECT user_id, profile.name
FROM (
	SELECT FROM_JSON(v.json, 'ARRAY<ARRAY<ROW<id INT, profile ROW<name STRING>>>>') AS nested
	FROM (VALUES 
        ROW('[[{"id":1, "profile": {"name":"Alice"}},{"id":2, "profile": {"name":"Bob"}}],[{"id":3, "profile": {"name":"Eve"}}]]')
    ) AS v(json)
)
CROSS JOIN UNNEST(nested) AS outer_arr(inner_arr) 
CROSS JOIN UNNEST(inner_arr) AS inner_row(user_id, profile);
```

## Building and testing

This is a Maven project. Typical build steps on a machine with Java and Maven installed:

1. run unit and integration tests

```bash
mvn test
```

2. Build the packaged UDF as JAR

```bash
mvn clean package
```

After a successful build the JAR is available under `target/` (for example `target/custom-json-udfs-1.0-SNAPSHOT.jar`). This is the artifact you can add to Flink's `lib/` folder or register with the SQL client using `ADD JAR '/your/path/to/custom-json-udfs-1.0-SNAPSHOT.jar';`.

If you want to skip tests during a quick build:

```bash
mvn clean package -DskipTests
```

## Register the UDF

1. Start the Flink SQL client (or use the web UI/sql-client entrypoint in a container).
2. Add the built JAR:

```sql
ADD JAR '/path/to/custom-json-udfs-1.0-SNAPSHOT.jar';
```

3. Create the function binding:

```sql
CREATE FUNCTION FROM_JSON AS 'com.github.hpgrahsl.flink.udfs.json.FromJsonUdf';
```

4. Run any of the examples above (copy/paste) that use `FROM_JSON(...)` in a `SELECT` query.


## Notes, limitations and tips

- The schema parser is intentionally minimal and supports the most common Flink data types which may or may not use parameters (e.g. `DECIMAL(p,s)`, `VARCHAR(n)`, `TIMESTAMP(p)`). Unrecognized types will typically cause an error.
- The UDF requires the schema parameter to be a literal string so Flink can determine the output type at planning time. If you try to pass a column reference or expression as the schema argument, you will get an error.
- JSON object keys are always strings; when parsing into `MAP<K,V>` the keys are converted from strings to the target key type (e.g. `INT`, `BIGINT`).
- Null or missing JSON fields map to SQL NULL.
