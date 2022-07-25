{{ config (
    materialized = "view",
) }}

SELECT
    block_number,
    address
FROM
    {{ ref("streamline__uniswap_contracts_by_date") }}
WHERE
    block_number <= 15000000
EXCEPT
SELECT
    block_number,
    address
FROM
    {{ ref("streamline__complete_uniswap_contracts") }}
WHERE
    block_number <= 15000000
