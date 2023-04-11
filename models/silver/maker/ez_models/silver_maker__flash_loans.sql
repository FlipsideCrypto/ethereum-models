{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        tx_hash,
        event_index,
        block_number,
        'SUCCESS' AS tx_status,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        receiver AS borrower,
        token AS token_loaned,
        amount,
        fee,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver_maker__dss_flashloan') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    tx_hash,
    event_index,
    block_number,
    tx_status,
    block_timestamp,
    origin_from_address,
    contract_address AS lender,
    borrower,
    token_loaned,
    amount AS amount_loaned,
    amount_loaned * pow(
        10,
        decimals
    ) AS amount_loaned_unadjusted,
    fee,
    symbol,
    decimals,
    _inserted_timestamp,
    _log_id
FROM
    base
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON C.address = token_loaned
