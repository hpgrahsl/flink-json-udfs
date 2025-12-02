ADD JAR '/opt/flink/lib/custom-json-udfs-1.0-SNAPSHOT.jar';

USE CATALOG default_catalog;

CREATE FUNCTION FROM_JSON AS 'com.github.hpgrahsl.flink.udfs.json.FromJsonUdf';

-- scenario 1: data gen with single JSON field
CREATE TABLE payloads_raw_1 (
    some_json STRING,
    create_ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'demo_topic_1',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-demo-topic-rt1',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE TABLE payloads_parsed_1 (
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
    'topic' = 'parsed_topic_1',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-demo-topic-pt1',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);
