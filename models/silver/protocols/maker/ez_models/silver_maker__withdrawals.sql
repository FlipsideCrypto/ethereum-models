{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address AS withdrawer,
        v_address AS vault,
        TRIM(SPLIT_PART(ilk, '-', 0)) AS symbol,
        dink * -1 AS amount_withdrawn,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver_maker__vat_frob') }}
    WHERE
        dink < 0
        AND dart = 0
        AND v_address <> '0xc73e0383f3aff3215e6f04b0331d58cecf0ab849' -- migration contract

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    'SUCCESS' AS tx_status,
    event_index,
    withdrawer,
    vault,
    token_address AS token_withdrawn,
    symbol,
    amount_withdrawn * pow(
        10,
        token_decimals
    ) AS amount_withdrawn_unadjusted,
    token_decimals AS decimals,
    amount_withdrawn,
    _inserted_timestamp,
    _log_id
FROM
    base d
    LEFT JOIN {{ ref('silver_maker__decimals') }} C
    ON token_symbol = symbol
