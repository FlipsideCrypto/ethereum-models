{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"contract_abis_v2",
        "sql_limit" :"100000",
        "producer_batch_size" :"18000",
        "worker_batch_size" :"18000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result"]) }
    ),
    tags = ['streamline_abis_realtime']
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
to_do AS (
    SELECT
        created_contract_address AS contract_address,
        block_number
    FROM
        {{ ref("silver__created_contracts") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
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
        )
        AND block_number IS NOT NULL
),
ready_abis AS (
    SELECT
        *
    FROM
        (
            SELECT
                contract_address,
                block_number
            FROM
                to_do
            UNION ALL
            SELECT
                contract_address,
                block_number
            FROM
                {{ ref("_retry_abis") }}
            WHERE
                block_number IS NOT NULL
        )
    WHERE
        contract_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        block_number DESC)) = 1
)
SELECT
    block_number,
    contract_address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://api.etherscan.io/api?module=contract&action=getabi&address=' || contract_address || '&apikey={key}',
        OBJECT_CONSTRUCT(
            'accept',
            'application/json'
        ),
        NULL,
        'vault/prod/ethereum/block_explorers/etherscan'
    ) AS request
FROM
    ready_abis
ORDER BY
    block_number DESC
