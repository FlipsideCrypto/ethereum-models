{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_committees()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['type', 'block_number', 'state_id']
    ) }} AS id,
    TYPE,
    block_number,
    state_id
FROM
    {{ ref("streamline__beacon_blocks") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        ) {# TODO: OR can be removed once historical load is complete #}
        OR block_number > 15000000
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    TYPE,
    block_number,
    state_id
FROM
    {{ ref("streamline__complete_eth_v2_committees") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    ) {# TODO: OR can be removed once historical load is complete #}
    OR block_number > 15000000
UNION ALL
SELECT
    id,
    TYPE,
    block_number,
    state_id
FROM
    {{ ref("streamline__eth_v2_committees_history") }}
