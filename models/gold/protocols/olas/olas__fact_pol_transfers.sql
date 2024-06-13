{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'LP, PROTOCOL OWNED LIQUIDITY' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    from_address,
    to_address,
    contract_address AS lp_token_address,
    lp_token_name,
    lp_token_amount_unadj,
    lp_token_amount_adj AS lp_token_amount,
    source_chain,
    pol_transfers_id AS fact_pol_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__pol_transfers') }}
