{{ config (
    materialized = "table",
    unique_key = "contract_address"
) }}

SELECT
    contract_address,
    'ethereum' AS blockchain,
    COUNT(*) AS events,
    MAX(block_number) AS latest_block
FROM
    {{ ref('silver__logs') }}
GROUP BY
    1,
    2
HAVING
    COUNT(*) >= 25
