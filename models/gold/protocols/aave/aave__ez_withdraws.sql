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
    "columns": true }
) }}
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    aave_market,
    aave_token,
    withdrawn_tokens,
    withdrawn_usd,
    depositor_address,
    aave_version,
    token_price,
    symbol,
    blockchain,
    _log_id,
    _inserted_timestamp,
    COALESCE (
        aave_withdraws_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }}
    ) AS ez_withdraws_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ref('silver__aave_withdraws')}}