{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "round(block_number,-3)",
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(abi_address, _log_id)"
) }}

WITH base AS (

    SELECT
        l.block_number,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        l.topics,
        l.data,
        l._log_id,
        p.proxy_address,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__proxies') }}
        p
        ON p.contract_address = l.contract_address
        AND l.block_number BETWEEN p.start_block
        AND p.end_block
    WHERE
        1 = 1

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_number,
    b._log_id,
    contract_address,
    proxy_address,
    COALESCE(
        proxy_address,
        contract_address
    ) AS abi_address,
    OBJECT_CONSTRUCT(
        'topics',
        b.topics,
        'data',
        b.data,
        'address',
        b.contract_address
    ) AS DATA,
    _inserted_timestamp
FROM
    base b
