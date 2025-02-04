{{ config(
    materialized = 'incremental',
    unique_key = ['address'],
    cluster_by = ['_inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address)",
    incremental_strategy = 'delete+insert',
    tags = ['curated']
) }}

SELECT
    block_number,
    block_timestamp,
    address,
    current_bal_unadj,
    _inserted_timestamp
FROM
    {{ source('ethereum_silver', 'eth_balance_diffs') }}
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
