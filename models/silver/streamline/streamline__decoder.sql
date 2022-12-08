{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "round(block_number,-3)",
    merge_update_columns = ["_log_id"],
) }}

WITH base AS (

    SELECT
        l.block_number,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        topics,
        l.data,
        A.data AS abi,
        _log_id
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__proxies') }}
        p
        ON p.contract_address = l.contract_address
        AND l.block_number BETWEEN p.start_block
        AND p.end_block
        LEFT JOIN {{ ref('silver__abis') }} A
        ON A.contract_address = COALESCE(
            proxy_address,
            l.contract_address
        )
        AND l.block_number BETWEEN A.start_block
        AND A.end_block
    WHERE
        1 = 1
        AND l.block_timestamp :: DATE >= '2022-10-01' --** remove this for full load **

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
    b.abi,
    OBJECT_CONSTRUCT(
        'topics',
        b.topics,
        'data',
        b.data,
        'address',
        b.contract_address
    ) AS DATA
FROM
    base b
WHERE
    abi IS NOT NULL
