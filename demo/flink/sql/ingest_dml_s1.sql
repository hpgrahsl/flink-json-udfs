USE CATALOG default_catalog;

INSERT INTO payloads_parsed_1
SELECT 
    payload.id,
    payload.profile.name AS profile_name,
    payload.profile.emails,
    payload.settings.theme,
    payload.settings.notify,
    payload.note,
    payload.last_scores,
    create_ts
    FROM (
    SELECT
        FROM_JSON(
            some_json,
            'ROW<id STRING, profile ROW<name STRING, emails ARRAY<STRING>>, settings ROW<theme STRING, notify BOOLEAN>, note STRING, last_scores ARRAY<DECIMAL(6,3)>>'
        ) AS payload,
        create_ts
    FROM payloads_raw_1
);
