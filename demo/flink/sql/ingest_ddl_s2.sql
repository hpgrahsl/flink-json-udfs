ADD JAR '/opt/flink/lib/custom-json-udfs-1.0-SNAPSHOT.jar';

USE CATALOG default_catalog;

CREATE FUNCTION FROM_JSON AS 'com.github.hpgrahsl.flink.udfs.json.FromJsonUdf';

-- scenario 2: data gen with same JSON info spread across 4 separate JSON fields
CREATE TABLE payloads_raw_2 (
    some_json_1 STRING,
    some_json_2 STRING,
    some_json_3 STRING,
    some_json_4 STRING,
    create_ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'demo_topic_2',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-demo-topic-rt2',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE TABLE payloads_parsed_2 (
    id STRING,
    profile_name STRING,
    emails ARRAY<STRING>,
    theme STRING,
    notify BOOLEAN,
    note STRING,
    last_scores ARRAY<DECIMAL(6,3)>,
    create_ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'parsed_topic_2',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-demo-topic-pt2',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);
