{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'contract_address'],
    cluster_by = ['_inserted_timestamp'],
    incremental_strategy = 'delete+insert',
    tags = ['curated']
) }}


SELECT
/* NO_CACHE */
    block_number,
    block_timestamp,
    address,
    contract_address,
    current_bal_unadj,
    _inserted_timestamp
FROM
    {{ source('ethereum_silver', 'token_balance_diffs') }}
WHERE
    _inserted_timestamp <= SYSDATE() - INTERVAL '1 day'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY address, contract_address
    ORDER BY
        block_number DESC
) = 1