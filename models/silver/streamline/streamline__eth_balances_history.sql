{{ config (
    materialized = "view",
) }}

SELECT
    block_number,
    address
FROM
    {{ ref("streamline__eth_balances_by_date") }}
WHERE
    block_number <= 15000000
EXCEPT
SELECT
    block_number,
    address
FROM
    {{ ref("streamline__complete_eth_balances") }}
WHERE
    block_number <= 15000000
ORDER BY
    block_number
