{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    enabled = false,
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH repayment_txs AS (

    SELECT
        tx_hash
    FROM
        {{ ref('silver_maker__vat_frob') }}
    WHERE
        dink = 0
        AND dart < 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
dai_burns AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'SUCCESS' AS tx_succeeded,
        event_index,
        origin_from_address AS payer,
        origin_to_address AS vault,
        contract_address AS token_paid,
        'DAI' AS symbol,
        18 AS decimals,
        raw_amount AS amount_paid_unadjusted,
        amount_paid_unadjusted / pow(
            10,
            18
        ) AS amount_paid,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        contract_address = '0x6b175474e89094c44da98b954eedeac495271d0f'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                repayment_txs
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_succeeded,
    event_index,
    payer,
    vault,
    token_paid,
    symbol,
    decimals,
    amount_paid_unadjusted,
    amount_paid,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS repayments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    dai_burns
