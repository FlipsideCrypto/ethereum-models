{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    cluster_by = 'address',
    merge_update_columns = ['max_block', '_inserted_timestamp'],
    tags = ['curated']
) }}

SELECT
/* NO_CACHE */
    address,
    MAX(block_number) AS max_block,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ ref('silver__eth_balances') }}

WHERE
    _inserted_timestamp < (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            ethereum_dev.silver.eth_balance_diffs
    )
{% if is_incremental() %}
    AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{this}}
    )
{% endif %}
GROUP BY
    address