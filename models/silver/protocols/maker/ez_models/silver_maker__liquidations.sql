{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    enabled = false,
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','maker']
) }}

WITH cat_bite AS (

    SELECT
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        TRIM(SPLIT_PART(ilk, '-', 0)) AS symbol,
        id AS auction_id,
        tab AS collateral_balance_unadjusted,
        ilk,
        art,
        urn_address AS vault_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver_maker__cat_bite') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
dog_bark AS (
    SELECT
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        TRIM(SPLIT_PART(ilk, '-', 0)) AS symbol,
        id AS auction_id,
        due AS collateral_balance_unadjusted,
        ilk,
        art,
        urn_address AS vault_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver_maker__dog_bark') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
all_events AS (
    SELECT
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        symbol,
        auction_id,
        collateral_balance_unadjusted,
        ilk,
        art,
        vault_address,
        _inserted_timestamp,
        _log_id
    FROM
        dog_bark
    UNION ALL
    SELECT
        tx_hash,
        event_index,
        block_number,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        symbol,
        auction_id,
        collateral_balance_unadjusted,
        ilk,
        art,
        vault_address,
        _inserted_timestamp,
        _log_id
    FROM
        cat_bite
)
SELECT
    tx_hash,
    event_index,
    block_number,
    block_timestamp,
    'SUCCESS' AS tx_succeeded,
    contract_address,
    origin_from_address AS liquidator,
    origin_to_address AS liquidated_wallet,
    symbol,
    auction_id,
    token_decimals AS decimals,
    collateral_balance_unadjusted,
    token_address AS collateral,
    art / pow(
        10,
        token_decimals
    ) AS normalized_stablecoin_debt,
    collateral_balance_unadjusted / pow(
        10,
        token_decimals
    ) AS collateral_balance,
    token_address AS token_loaned,
    vault_address AS vault,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_events
    LEFT JOIN {{ ref('silver_maker__decimals') }} C
    ON symbol = token_symbol
