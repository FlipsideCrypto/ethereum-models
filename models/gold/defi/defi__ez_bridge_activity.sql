{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ACROSS, ALLBRIDGE, AXELAR, CELER, CBRIDGE, HOP, MESON, MULTICHAIN, NATIVE, STARGATE, SYMBIOSIS, SYNAPSE, WORMHOLE',
    'PURPOSE': 'BRIDGE' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    sender,
    receiver,
    destination_chain_receiver,
    COALESCE(
        standard_destination_chain,
        b.destination_chain
    ) AS destination_chain,
    destination_chain_id,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    COALESCE (
        complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_id']
        ) }}
    ) AS ez_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_bridge__complete_bridge_activity') }}
    b
    LEFT JOIN {{ ref('silver_bridge__standard_dst_chain_seed') }} C
    ON b.destination_chain = C.destination_chain
