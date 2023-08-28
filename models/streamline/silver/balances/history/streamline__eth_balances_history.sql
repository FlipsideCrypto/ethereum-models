{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_eth_balances(object_construct('node_name','quicknode', 'sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_balances_history']
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
),
traces AS (
    SELECT
        block_number,
        from_address,
        to_address
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number > 17000000
),
stacked AS (
    SELECT
        DISTINCT block_number,
        from_address AS address
    FROM
        traces
    WHERE
        from_address IS NOT NULL
        AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        to_address AS address
    FROM
        traces
    WHERE
        to_address IS NOT NULL
        AND to_address <> '0x0000000000000000000000000000000000000000'
)
SELECT
    block_number,
    address
FROM
    stacked
WHERE
    block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    address
FROM
    {{ ref("streamline__complete_eth_balances") }}
WHERE
    block_number < (
        SELECT
            block_number
        FROM
            last_3_days
    )
    AND block_number > 17000000
