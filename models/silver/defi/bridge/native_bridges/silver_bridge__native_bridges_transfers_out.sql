{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH bridges AS (

    SELECT
        LOWER(contract_address) AS bridge_address,
        LOWER(contract_name) AS bridge_name,
        LOWER(blockchain) AS blockchain
    FROM
        {{ ref('silver__native_bridges_seed') }}
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
        {{ ref('silver__transfers') }}
        t
        INNER JOIN bridges b
        ON t.to_address = b.bridge_address
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    'Transfer' AS event_name,
    bridge_address,
    bridge_name,
    from_address AS sender,
    to_address AS receiver,
    raw_amount AS amount_unadj,
    blockchain AS destination_chain,
    contract_address AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    token_transfers
