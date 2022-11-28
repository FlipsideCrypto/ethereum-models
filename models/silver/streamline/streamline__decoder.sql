{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "round(block_number,-3)",
    merge_update_columns = ["_log_id"],
) }}

WITH base AS (

    SELECT
        l.tx_hash,
        l.block_number,
        l.contract_address,
        p.proxy_address,
        topics,
        l.data,
        A.data AS abi,
        l._inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__proxies') }}
        p
        ON p.contract_address = l.contract_address
        AND l.block_number >= p.block_number
        AND l.block_number <= p.next_block_number
        LEFT JOIN {{ ref('silver__abis') }} A
        ON A.contract_address = COALESCE(
            proxy_address,
            l.contract_address
        )
    WHERE
        1 = 1
        AND l.block_timestamp :: DATE >= '2022-10-01' --remove this for full sync

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
    tx_hash,
    block_number,
    contract_address,
    proxy_address,
    topics,
    DATA,
    abi,
    _inserted_timestamp,
    _log_id
FROM
    base
WHERE
    abi IS NOT NULL
