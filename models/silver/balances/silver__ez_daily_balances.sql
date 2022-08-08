{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_date AS balance_date,
    user_address,
    contract_address,
    symbol,
    non_adjusted_balance,
    balance,
    balance_usd,
    decimals,
    NAME,
    has_decimal,
    has_price
FROM
    {{ ref('silver__daily_token_balances') }}
UNION
SELECT
    block_date AS balance_date,
    user_address,
    NULL AS contract_address,
    symbol,
    non_adjusted_balance,
    balance,
    balance_usd,
    decimals,
    NAME,
    has_decimal,
    has_price
FROM
    {{ ref('silver__daily_eth_balances') }}
