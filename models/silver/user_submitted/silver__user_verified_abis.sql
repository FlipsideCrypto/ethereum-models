{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"]
) }}

WITH base AS (

    SELECT
        contract_address,
        abi,
        user_submitting,
        _inserted_timestamp
    FROM
        {{ source(
            "eth_dev_db_user",
            "user_abis"
        ) }}
    WHERE
        blockchain = 'ethereum'

{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
)
{% endif %}
LIMIT
    10
), proxy_contracts AS (
    SELECT
        p.contract_address,
        COALESCE(
            p.proxy_address,
            p.contract_address
        ) AS abi_address,
        p.start_block,
        p.end_block,
        abi,
        user_submitting,
        b._inserted_timestamp
    FROM
        {{ ref('silver__proxies') }}
        p
        JOIN base b
        ON b.contract_address = p.proxy_address
),
non_proxy_contracts AS (
    SELECT
        contract_address,
        contract_address AS abi_address,
        0 AS start_block,
        pow(
            10,
            18
        ) AS end_block,
        abi,
        user_submitting,
        _inserted_timestamp
    FROM
        base
    WHERE
        contract_address NOT IN (
            SELECT
                abi_address
            FROM
                proxy_contracts
        )
),
all_contracts AS (
    SELECT
        contract_address,
        abi_address,
        start_block,
        end_block,
        abi,
        user_submitting,
        _inserted_timestamp
    FROM
        proxy_contracts
    UNION ALL
    SELECT
        contract_address,
        abi_address,
        start_block,
        end_block,
        abi,
        user_submitting,
        _inserted_timestamp
    FROM
        non_proxy_contracts
),
logs AS (
    SELECT
        l.block_number,
        l.contract_address,
        l.topics AS logs_topics,
        l.data AS logs_data,
        C.abi,
        abi_address
    FROM
        {{ ref('silver__logs') }}
        l
        JOIN all_contracts C
        ON C.contract_address = l.contract_address
        AND l.block_number BETWEEN C.start_block
        AND C.end_block
),
recent_logs AS (
    SELECT
        block_number,
        contract_address,
        logs_topics,
        logs_data,
        abi,
        abi_address
    FROM
        logs qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        block_number DESC)) <= 500
),
decoded_logs AS (
    SELECT
        *,
        ethereum.streamline.udf_decode(PARSE_JSON(abi), OBJECT_CONSTRUCT('topics', logs_topics, 'data', logs_data, 'address', contract_address)) AS decoded_output,
        decoded_output [0] :name :: STRING AS decoded_event_name
    FROM
        recent_logs
),
successful_abis AS (
    SELECT
        abi_address,
        SUM(
            CASE
                WHEN decoded_event_name IS NOT NULL THEN 1
            END
        ) AS successful_rows,
        COUNT(*) AS total_rows,
        successful_rows / total_rows AS success_rate
    FROM
        decoded_logs
    GROUP BY
        1
)
SELECT
    contract_address,
    abi,
    user_submitting,
    _inserted_timestamp
FROM
    base
WHERE
    contract_address IN (
        SELECT
            abi_address
        FROM
            successful_abis
        WHERE
            success_rate > 0.75
    )
