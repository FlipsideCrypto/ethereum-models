{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER, DODO, FRAX, HASHFLOW, KYBERSWAP, MAVERICK, PANCAKESWAP, SHIBASWAP, TRADER JOE, VERSE, WOOFI',
    'PURPOSE': 'DEX, SWAPS' } } },
    tags = ['gold','defi','dex','curated','ez']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    ROUND(
        CASE
            WHEN token_in <> '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' or not token_in_is_verified
            AND (
                amount_out_usd IS NULL
                OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.75
                OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.75
            ) THEN NULL
            ELSE amount_in_usd
        END,
        2
    ) AS amount_in_usd,
    amount_out_unadj,
    amount_out,
    ROUND(
        CASE
            WHEN token_out <> '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' or not token_out_is_verified
            AND (
                amount_in_usd IS NULL
                OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.75
                OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.75
            ) THEN NULL
            ELSE amount_out_usd
        END,
        2
    ) AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version as protocol_version,
    token_in,
    token_in_is_verified,
    token_out,
    token_out_is_verified,
    symbol_in,
    symbol_out,
    COALESCE (
        complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index']
        ) }}
    ) AS ez_dex_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_dex__complete_dex_swaps') }}
