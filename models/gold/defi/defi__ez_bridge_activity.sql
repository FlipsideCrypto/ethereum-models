{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 
        'database_tags':{
            'table':{
                'PROTOCOL': 'ACROSS, ALLBRIDGE, AXELAR, SQUID, CELER, CBRIDGE, HOP, MULTICHAIN, NATIVE, STARGATE, SYMBIOSIS, SYNAPSE, WORMHOLE',
                'PURPOSE': 'BRIDGE'
        } } }
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
    destination_chain,
    destination_chain_id,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd
FROM
    {{ ref('silver_bridge__complete_bridge_activity') }}
