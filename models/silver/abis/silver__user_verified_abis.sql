{{ config (
    materialized = "incremental",
    unique_key = "id",
    merge_update_columns = ["id"]
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
), max_blocks AS (
    SELECT
        abi_address,
        abi,
        MAX(block_number) AS max_block,
        max_block - 100000 AS min_block
    FROM
        {{ ref('streamline__decode_logs') }}
        l
        JOIN base C
        ON C.contract_address = l.abi_address
    GROUP BY
        1,
        2
),
logs AS (
    SELECT
        l.block_number,
        l.contract_address,
        l.data AS logs_data,
        C.abi,
        l.abi_address
    FROM
        {{ ref('streamline__decode_logs') }}
        l
        JOIN max_blocks C
        ON C.abi_address = l.abi_address
        AND l.block_number BETWEEN C.min_block
        AND C.max_block
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
