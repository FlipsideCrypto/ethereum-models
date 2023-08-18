{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    tags = ['non_realtime']
) }}

WITH bridges AS (

    SELECT
        LOWER(contract_address) AS bridge_address,
        LOWER(contract_name) AS bridge_name,
        LOWER(blockchain) AS blockchain
    FROM {{ ref('silver__native_bridges_seed') }}
),
token_transfers AS (
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
        bridge_address,
        bridge_name,
        blockchain,
        raw_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }} t 
    INNER JOIN bridges b ON t.to_address = b.bridge_address

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
        bridge_address,
        bridge_name,
        blockchain,
        eth_value,
        identifier,
        input,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__eth_transfers') }} t 
    INNER JOIN bridges b ON t.to_address = b.bridge_address

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
    bridge_address,
    raw_amount AS amount,
    bridge_name,
    blockchain,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id']
    ) }} AS _id,
    _inserted_timestamp
FROM
    token_transfers
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    'ETH' AS token_address,
    from_address,
    bridge_address,
    eth_value AS amount,
    bridge_name,
    blockchain,
    {{ dbt_utils.generate_surrogate_key(
        ['_call_id']
    ) }} AS _id,
    _inserted_timestamp
FROM
    native_transfers
