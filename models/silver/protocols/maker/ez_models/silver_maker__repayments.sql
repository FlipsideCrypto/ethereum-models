{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
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
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
dai_burns AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'SUCCESS' AS tx_status,
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
    tx_status,
    event_index,
    payer,
    vault,
    token_paid,
    symbol,
    decimals,
    amount_paid_unadjusted,
    amount_paid,
    _inserted_timestamp,
    _log_id
FROM
    dai_burns
