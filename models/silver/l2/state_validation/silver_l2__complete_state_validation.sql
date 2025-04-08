{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE','platform_name'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, event_type, platform_address, platform_exchange_version, seller_address, buyer_address, nft_address, project_name, currency_address, currency_symbol), SUBSTRING(origin_function_signature, event_type, platform_address, platform_exchange_version, seller_address, buyer_address, nft_address, project_name, currency_address, currency_symbol)",
    tags = ['curated','reorg', 'heal']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        event_index,
        tx_hash,
        origin_from_address,
        contract_address,
        root_claim AS validation_data,
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
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    contract_address,
    root_claim AS validation_data,
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
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    contract_address,
    root_claim AS validation_data,
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
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
    contract_address,
    chain,
    chain_category,
    validation_address,
    validation_type,
    validation_data,
    validation_data_type,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'chain','validation_type']
    ) }} AS complete_state_validation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
