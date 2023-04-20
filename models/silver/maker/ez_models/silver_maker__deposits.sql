{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address AS depositor,
        v_address AS vault,
        TRIM(SPLIT_PART(ilk, '-', 0)) AS symbol,
        dink AS amount_deposited,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver_maker__vat_frob') }}
    WHERE
        dink > 0
        AND dart = 0

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
    depositor,
    vault,
    token_address AS token_deposited,
    symbol,
    amount_deposited * pow(
        10,
        token_decimals
    ) AS amount_deposited_unadjusted,
    token_decimals AS decimals,
    amount_deposited,
    _inserted_timestamp,
    _log_id
FROM
    base d
    LEFT JOIN {{ ref('silver_maker__decimals') }} C
    ON token_symbol = symbol
