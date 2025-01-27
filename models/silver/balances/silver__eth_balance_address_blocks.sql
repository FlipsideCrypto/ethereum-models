{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    cluster_by = 'address',
    merge_update_columns = ['block_number', '_inserted_timestamp', 'prev_bal_unadj', 'current_bal_unadj'],
    tags = ['curated']
) }}

SELECT
/* NO_CACHE */
    block_number,
    block_timestamp,
    address,
    prev_bal_unadj,
    current_bal_unadj,
    _inserted_timestamp
FROM
    ethereum_dev.silver.eth_balance_diffs
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
    PARTITION BY address
    ORDER BY
        block_number DESC
) = 1
