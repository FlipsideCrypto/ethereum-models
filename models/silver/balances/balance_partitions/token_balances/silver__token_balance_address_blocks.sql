{{ config(
    materialized = 'table',
    unique_key = ['address', 'contract_address'],
    cluster_by = ['max_block_partition', 'address', 'contract_address'],
    tags = ['curated']
) }}

SELECT
    address,
    contract_address,
    MAX(block_number) AS max_block,
    CASE
        WHEN MAX(block_number) < 5000000 THEN '0_5m'
        WHEN MAX(block_number) < 10000000 THEN '5m_10m'
        WHEN MAX(block_number) < 15000000 THEN '10m_15m'
        WHEN MAX(block_number) < 20000000 THEN '15m_20m'
        ELSE '20m_plus'
    END AS max_block_partition
FROM
    {{ ref('silver__token_balances') }}
WHERE
    _inserted_timestamp < (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            ethereum_dev.silver.token_balance_diffs
    )
GROUP BY
    1,
    2
