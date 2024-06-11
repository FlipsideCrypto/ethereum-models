{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'LIQUIDITY, BONDING' } } }
) }}

SELECT
    C.block_number,
    C.block_timestamp,
    C.tx_hash,
    C.origin_function_signature,
    C.origin_from_address,
    C.origin_to_address,
    C.contract_address,
    C.event_index,
    C.event_name,
    C.owner_address,
    '0x0001a500a6b18995b03f44bb040a5ffc28e45cb0' AS token_address,
    C.amount_olas_unadj AS olas_amount_unadj,
    C.amount_olas_adj AS olas_amount,
    C.amount_olas_adj * p.price AS olas_amount_usd,
    C.token_address AS lp_token_address,
    C.token_amount_unadj AS lp_amount_unadj,
    C.token_amount_adj AS lp_amount,
    C.product_id,
    C.bond_id,
    C.maturity_timestamp,
    CASE
        WHEN r.bond_id IS NULL THEN FALSE
        ELSE TRUE
    END AS is_redeemed,
    C.create_bond_id AS ez_olas_bonding_id,
    C.inserted_timestamp,
    C.modified_timestamp
FROM
    {{ ref('silver_olas__create_bond') }} C
    LEFT JOIN {{ ref('silver_olas__redeem_bond') }}
    r USING(bond_id)
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON p.token_address = '0x0001a500a6b18995b03f44bb040a5ffc28e45cb0'
    AND DATE_TRUNC(
        'hour',
        C.block_timestamp
    ) = p.hour
