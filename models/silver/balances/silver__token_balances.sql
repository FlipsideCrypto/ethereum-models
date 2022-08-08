{{ config(
    materialized = 'incremental',
    unique_key = 'address || block_number || contract_address',
    cluster_by = ['block_date'],
    tags = ['balances']
) }}

WITH base_table AS (

    SELECT
        block_number,
        address,
        contract_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                VALUE :data :result :: STRING
            )
        ) AS balance
    FROM
        {{ source(
            'dev_db',
            'atb_balances_test'
        ) }}
        qualify(ROW_NUMBER() over(PARTITION BY address, block_number, contract_address
    ORDER BY
        balance DESC)) = 1 -- remove qualify, add incremental logic
),
block_date AS (
    SELECT
        block_date,
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
)
SELECT
    A.block_number,
    b.block_date,
    address,
    contract_address,
    balance
FROM
    base_table A
    LEFT JOIN block_date b
    ON A.block_number = b.block_number
