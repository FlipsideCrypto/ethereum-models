{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source','{{this.identifier}}', 'external_table','beacon_validators', 'route','validators', 'producer_batch_size', 200,'producer_limit_size', 100000, 'worker_batch_size', 20, 'producer_batch_chunks_size', 2))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH max_slot_number AS (

    SELECT
        MAX(slot_number) AS max_slot_number
    FROM
        {{ this }}
),
base_data AS (
    SELECT
        slot_number,
        VALUE,
        DATA
    FROM
        {{ source(
            'bronze_streamline',
            'beacon_blocks'
        ) }}

{% if is_incremental() %}
WHERE
    (
        slot_number >= (
            SELECT
                MAX(max_slot_number)
            FROM
                max_slot_number
        )
    )
{% endif %}
),
raw_tbl AS (
    SELECT
        MD5(
            CAST(COALESCE(CAST(slot_number AS text), '') AS text)
        ) AS id,
        TO_TIMESTAMP(
            DATA :message :body :execution_payload :timestamp :: INTEGER
        ) AS slot_timestamp,
        slot_number,
        VALUE :data :message :state_root :: STRING AS state_id
    FROM
        base_data
    WHERE
        VALUE :data :message :state_root :: STRING IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4
),
join_tbl AS (
    SELECT
        MAX(slot_number) AS slot_number,
        DATE(slot_timestamp) AS slot_timestamp
    FROM
        raw_tbl
    GROUP BY
        2
),
max_block_date_tbl AS (
    SELECT
        raw_tbl.id AS id,
        raw_tbl.slot_number AS slot_number,
        raw_tbl.slot_timestamp AS slot_timestamp,
        raw_tbl.state_id AS state_id
    FROM
        raw_tbl
        INNER JOIN join_tbl
        ON raw_tbl.slot_number = join_tbl.slot_number
        AND DATE(
            raw_tbl.slot_timestamp
        ) = join_tbl.slot_timestamp
)
SELECT
    slot_number,
    state_id
FROM
    max_block_date_tbl
EXCEPT
SELECT
    slot_number,
    state_id
FROM
    {{ ref("streamline__complete_beacon_validators") }}
