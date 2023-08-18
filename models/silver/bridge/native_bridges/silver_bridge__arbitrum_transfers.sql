{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    tags = ['non_realtime']
) }}

WITH token_transfers AS (

SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    _log_id,
    _inserted_timestamp
FROM {{ ref('silver__transfers') }}
WHERE to_address IN (
    '0xcee284f754e854890e311e3280b767f80797180d', --Arbitrum One: L1 Arb - Custom Gateway
    '0xa3a7b6f88361f48403514059f1f16c8e78d60eec', --Arbitrum One: L1 ERC20 Gateway
    '0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f' --Arbitrum: Delayed Inbox
    )
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
),

native_transfers AS (

SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    eth_value,
    identifier,
    input,
    _call_id,
    _inserted_timestamp
FROM {{ ref('silver__eth_transfers') }}
WHERE to_address IN (
    '0xcee284f754e854890e311e3280b767f80797180d', --Arbitrum One: L1 Arb - Custom Gateway
    '0xa3a7b6f88361f48403514059f1f16c8e78d60eec', --Arbitrum One: L1 ERC20 Gateway
    '0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f' --Arbitrum: Delayed Inbox
    )
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}

)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address AS token_address,
    from_address,
    to_address AS bridge_address,
    raw_amount AS amount,
    'arbitrum' AS name,
    _log_id AS _id,
    _inserted_timestamp
FROM token_transfers
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    'ETH' AS token_address,
    from_address,
    to_address AS bridge_address,
    eth_value AS amount,
    'arbitrum' AS name,
    _call_id AS _id,
    _inserted_timestamp
FROM native_transfers