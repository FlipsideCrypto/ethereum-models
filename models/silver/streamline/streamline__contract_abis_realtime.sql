{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_contract_abis()",
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
        ['block_number', 'contract_address']
    ) }} AS id,
    contract_address,
    block_number
FROM
    {{ ref("streamline__contract_addresses") }}
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
    contract_address,
    block_number
FROM
    {{ ref("streamline__complete_contract_abis") }}
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
    contract_address,
    block_number
FROM
    {{ ref("streamline__contract_abis_history") }}
