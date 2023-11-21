{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true },
) }}

  SELECT
    block_hour,
    block_number,
    aave_market,
    lending_pool_add,
    data_provider,
    reserve_name,
    atoken_address,
    stable_debt_token_address,
    variable_debt_token_address,
    reserve_price,
    atoken_price,
    total_liquidity_token,
    total_liquidity_usd,
    total_stable_debt_token,
    total_stable_debt_usd,
    total_variable_debt_token,
    total_variable_debt_usd,
    supply_rate,
    borrow_rate_stable,
    borrow_rate_variable,
    aave_price,
    utilization_rate,
    aave_version,
    blockchain,
    stkaave_rate_supply,
    stkaave_rate_variable_borrow,
    _inserted_timestamp,
    read_id,
    COALESCE (
        aave_market_stats_id,
        {{ dbt_utils.generate_surrogate_key(
          ['block_number', 'aave_version', 'aave_market']
        ) }}
    ) AS ez_market_stats_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp

  FROM
    {{ ref('silver__blocks') }}

