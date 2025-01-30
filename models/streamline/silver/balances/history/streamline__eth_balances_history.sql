{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"eth_balances_v2",
        "sql_limit" :"173000000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}" }
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
        {{ ref('core__fact_traces') }}
    WHERE
        value > 0
        AND trace_succeeded
        AND tx_succeeded
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
),
to_do AS (
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
)
SELECT
    block_number,
    address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            CONCAT(
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBalance',
            'params',
            ARRAY_CONSTRUCT(address, utils.udf_int_to_hex(block_number))),
            'vault/prod/ethereum/quicknode/mainnet'
        ) AS request
        FROM
            to_do
        ORDER BY
            partition_key ASC