{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','chain', 'validation_type'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, chain, chain_category, validation_address, validation_type)",
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        event_index,
        tx_hash,
        origin_from_address,
        origin_to_address,
        contract_address,
        batch_root AS validation_data,
        validation_data_json,
        'state_root_proposal' AS validation_data_type,
        chain,
        chain_category,
        validation_address,
        validation_type,
        inserted_timestamp
    FROM
        {{ ref('silver_l2__opstack_legacy_state') }}

{% if is_incremental() and 'optimism_legacy' not in var('HEAL_MODELS') %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    output_root AS validation_data,
    validation_data_json,
    'state_root_proposal' AS validation_data_type,
    chain,
    chain_category,
    validation_address,
    validation_type,
    inserted_timestamp
FROM
    {{ ref('silver_l2__opstack_output_oracle') }}

{% if is_incremental() and 'output_oracle' not in var('HEAL_MODELS') %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    root_claim AS validation_data,
    validation_data_json,
    'state_root_proposal' AS validation_data_type,
    chain,
    chain_category,
    validation_address,
    validation_type,
    inserted_timestamp
FROM
    {{ ref('silver_l2__opstack_dispute_games') }}

{% if is_incremental() and 'dispute_games' not in var('HEAL_MODELS') %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    origin_from_address,
    origin_to_address,
    contract_address,
    chain,
    chain_category,
    validation_address,
    validation_type,
    validation_data,
    validation_data_type,
    validation_data_json,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'chain','validation_type']
    ) }} AS complete_state_validation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
