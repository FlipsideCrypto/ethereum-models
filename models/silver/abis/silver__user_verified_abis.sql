{{ config (
    materialized = "incremental",
    unique_key = "id",
    merge_update_columns = ["id"],
    tags = ['abis']
) }}

WITH base AS (

    SELECT
        contract_address,
        abi,
        SHA2(PARSE_JSON(abi)) AS abi_hash,
        discord_username,
        _inserted_timestamp
    FROM
        {{ source(
            "crosschain_public",
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
ORDER BY
    _inserted_timestamp ASC
LIMIT
    10
), contracts AS (
    SELECT
        contract_address
    FROM
        {{ ref('silver__proxies') }}
    WHERE
        contract_address IN (
            SELECT
                contract_address
            FROM
                base
        )
),
proxies AS (
    SELECT
        proxy_address,
        contract_address
    FROM
        {{ ref('silver__proxies') }}
    WHERE
        proxy_address IN (
            SELECT
                contract_address
            FROM
                base
        )
),
final_groupings AS (
    SELECT
        b.contract_address AS address,
        C.contract_address,
        proxy_address,
        CASE
            WHEN C.contract_address IS NOT NULL
            AND proxy_address IS NOT NULL THEN 'contract'
            WHEN C.contract_address IS NOT NULL THEN 'contract'
            WHEN proxy_address IS NOT NULL THEN 'proxy'
            WHEN C.contract_address IS NULL
            AND proxy_address IS NULL THEN 'contract'
        END AS TYPE,
        p.contract_address AS proxy_parent,
        CASE
            WHEN TYPE = 'contract' THEN address
            ELSE proxy_parent
        END AS final_address
    FROM
        base b
        LEFT JOIN (
            SELECT
                DISTINCT contract_address
            FROM
                contracts
        ) C
        ON b.contract_address = C.contract_address
        LEFT JOIN (
            SELECT
                DISTINCT proxy_address,
                contract_address
            FROM
                proxies
        ) p
        ON b.contract_address = proxy_address
),
identified_addresses AS (
    SELECT
        DISTINCT address AS base_address,
        final_address AS contract_address
    FROM
        final_groupings
),
ranges AS (
    SELECT
        contract_address,
        base_address,
        MIN(block_number) AS min_block,
        min_block + 100000 AS max_block
    FROM
        {{ ref('silver__logs') }}
        JOIN identified_addresses USING (contract_address)
    GROUP BY
        contract_address,
        base_address
),
logs AS (
    SELECT
        l.block_number,
        l.contract_address,
        OBJECT_CONSTRUCT(
            'topics',
            l.topics,
            'data',
            l.data,
            'address',
            l.contract_address
        ) AS logs_data,
        b.abi,
        base_address AS abi_address
    FROM
        {{ ref('silver__logs') }}
        l
        JOIN ranges C
        ON C.contract_address = l.contract_address
        AND l.block_number BETWEEN C.min_block
        AND C.max_block
        JOIN base b
        ON b.contract_address = C.base_address
),
recent_logs AS (
    SELECT
        block_number,
        contract_address,
        logs_data,
        abi,
        abi_address
    FROM
        logs qualify(ROW_NUMBER() over(PARTITION BY abi_address
    ORDER BY
        block_number DESC)) BETWEEN 0
        AND 500
),
decoded_logs AS (
    SELECT
        *,
        ethereum.streamline.udf_decode(PARSE_JSON(abi), logs_data) AS decoded_output,
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
