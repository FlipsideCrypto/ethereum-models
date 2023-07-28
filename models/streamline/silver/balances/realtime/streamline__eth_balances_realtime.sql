{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_eth_balances(object_construct('node_name','flipsidenode', 'sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_balances_real_time']
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
    block_number,
    address
FROM
    {{ ref("streamline__eth_balances") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    address
FROM
    {{ ref("streamline__complete_eth_balances") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
    AND block_number IS NOT NULL
