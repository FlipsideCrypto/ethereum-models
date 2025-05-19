{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    enabled = false,
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','maker']
) }}

WITH base AS (

    SELECT
        tx_hash,
        event_index,
        block_number,
        'SUCCESS' AS tx_succeeded,
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
    tx_succeeded,
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
    b._inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS flash_loans_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base b 
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON C.address = token_loaned
