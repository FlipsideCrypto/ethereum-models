{{ config (
    materialized = "incremental",
    unique_key = "id",
    merge_update_columns = ["id"]
) }}

WITH base AS (

    SELECT
        contract_address,
        abi,
        SHA2(parse_json(abi)) AS abi_hash,
        discord_username,
        _inserted_timestamp
    FROM
        {{ source(
            "eth_bronze_public",
            "user_abis"
        ) }}
    WHERE
        blockchain = 'ethereum'
        AND NOT duplicate_abi

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
        discord_username,
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
        discord_username,
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
        discord_username,
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
        discord_username,
        _inserted_timestamp
    FROM
        non_proxy_contracts
),
block_range AS (
    SELECT
        l.contract_address,
        MAX(block_number) AS max_b,
        max_b - 100000 AS min_b
    FROM
        {{ ref('silver__logs') }}
        l
        JOIN all_contracts C
        ON C.contract_address = l.contract_address
        AND l.block_number BETWEEN C.start_block
        AND C.end_block
    GROUP BY
        1
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
        JOIN block_range b
        ON b.contract_address = l.contract_address
        AND l.block_number >= b.min_b
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
        block_number DESC)) BETWEEN 100
        AND 600
),
decoded_logs AS (
    SELECT
        *,
        ethereum.streamline.udf_decode(PARSE_JSON(abi), OBJECT_CONSTRUCT('topics', logs_topics, 'data', logs_data, 'address', contract_address)) AS decoded_output,
        decoded_output [0] :decoded :: BOOLEAN AS decoded,
        CASE
            WHEN decoded THEN 1
            ELSE 0
        END AS successful_row
    FROM
        recent_logs
),
successful_abis AS (
    SELECT
        abi_address,
        SUM(successful_row) AS successful_rows,
        COUNT(*) AS total_rows,
        successful_rows / total_rows AS success_rate
    FROM
        decoded_logs
    GROUP BY
        abi_address
)
SELECT
    contract_address,
    abi,
    discord_username,
    _inserted_timestamp,
    abi_hash,
    CONCAT(
        contract_address,
        '-',
        abi_hash
    ) AS id
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