{{ config (
    materialized = "view",
    primary_key = "id",
    post_hook = "call {{this.schema}}.sp_get_{{this.identifier}}()"
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("max_block_by_date") }}
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
    address,
    contract_address
FROM
    {{ ref("streamline__token_balances_by_date") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    ) {# TODO: OR can be removed once historical load is complete #}
    OR block_number > 15000000
EXCEPT
SELECT
    block_number,
    address,
    contract_address
FROM
    {{ ref("streamline__complete_token_balances") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    ) {# TODO: OR can be removed once historical load is complete #}
    OR block_number > 15000000
