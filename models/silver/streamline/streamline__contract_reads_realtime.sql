{{ config (
    materialized = "view",
    primary_key = "id",
    post_hook = "call {{this.schema}}.sp_get_{{this.identifier}}()"
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_number
            ORDER BY
                block_number DESC
        ) = 3
    LIMIT
        1
)
SELECT
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    {{ ref("streamline__contract_reads") }}
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
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    {{ ref("streamline__complete_contract_reads") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    ) {# TODO: OR can be removed once historical load is complete #}
    OR block_number > 15000000
