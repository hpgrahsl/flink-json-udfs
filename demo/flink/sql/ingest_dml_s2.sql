USE CATALOG default_catalog;

INSERT INTO payloads_parsed_2
SELECT 
    payload_1.id,
    payload_2.profile.name AS profile_name,
    payload_2.profile.emails,
    payload_3.settings.theme,
    payload_3.settings.notify,
    payload_1.note,
    payload_4.last_scores,
    create_ts
    FROM (
    SELECT
        FROM_JSON(
            some_json_1,
            'ROW<id STRING, note STRING>'
        ) AS payload_1,
        FROM_JSON(
            some_json_2,
            'ROW<profile ROW<name STRING, emails ARRAY<STRING>>>'
        ) AS payload_2,
        FROM_JSON(
            some_json_3,
            'ROW<settings ROW<theme STRING, notify BOOLEAN>>'
        ) AS payload_3,
        FROM_JSON(
            some_json_4,
            'ROW<last_scores ARRAY<DECIMAL(6,3)>>'
        ) AS payload_4,
        create_ts
    FROM payloads_raw_2
);
