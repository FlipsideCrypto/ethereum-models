{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"token_balances_v2",
        "sql_limit" :"1000000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"10000",
        "async_concurrent_requests" :"10",
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
relevant_contracts AS (
    select contract_address, count(*)
    from {{ ref("core__ez_token_transfers") }}
    where block_timestamp > current_date() - 60 and token_is_verified
    group by all
    order by 2 desc 
    limit 100
),
logs as (
    select 
    to_address as address1,
    from_address as address2,
    contract_address,
    block_number
    from {{ ref("core__ez_token_transfers") }}
    where contract_address in (select contract_address from relevant_contracts union select '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
    and block_number > 21000000
    and block_number < (select block_number from last_3_days)
),
transfers AS (
    SELECT
        DISTINCT block_number,
        contract_address,
        address1 AS address
    FROM
        logs
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        contract_address,
        address2 AS address
    FROM
        logs
    WHERE
        address2 IS NOT NULL
        AND address2 <> '0x0000000000000000000000000000000000000000'
),
to_do AS (
    SELECT
        block_number,
        address,
        contract_address
    FROM
        transfers
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        address,
        contract_address
    FROM
        {{ ref("streamline__complete_token_balances") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
        AND block_number > 21000000
)
SELECT
    block_number,
    address,
    contract_address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id',
            CONCAT(
                contract_address,
                '-',
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_call',
            'params',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to',
                    contract_address,
                    'data',
                    CONCAT(
                        '0x70a08231000000000000000000000000',
                        SUBSTR(
                            address,
                            3
                        )
                    )
                ),
                utils.udf_int_to_hex(block_number)
            )
        ),
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS request
FROM
    to_do
ORDER BY
    partition_key ASC

limit 1000000