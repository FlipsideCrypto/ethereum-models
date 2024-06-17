{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'GOVERNANCE, VEOLAS, BUOLAS, LOCK, UNLOCK' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    contract_name,
    event_index,
    event_name,
    account_address,
    amount_olas_unadj AS olas_amount_unadj,
    amount_olas_adj AS olas_amount,
    amount_olas_adj * p.price AS olas_amount_usd,
    CASE
        WHEN event_name = 'Deposit' THEN end_time_timestamp
        WHEN event_name = 'Withdraw' THEN NULL
    END AS unlock_timestamp,
    olas_locking_id AS ez_olas_locking_id,
    d.inserted_timestamp,
    GREATEST(
        d.modified_timestamp,
        p.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__olas_locking') }}
    d
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON p.token_address = '0x0001a500a6b18995b03f44bb040a5ffc28e45cb0'
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p.hour
