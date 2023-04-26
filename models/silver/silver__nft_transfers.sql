{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core']
) }}

WITH base AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address,
        event_name,
        topics,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND (
            (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND DATA = '0x'
                AND topics [3] IS NOT NULL
            ) --erc721s
            OR (
                topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) --erc1155s
            OR (
                topics [0] :: STRING IN (
                    '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3',
                    '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8'
                )
                AND contract_address IN (
                    '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb',
                    '0xb8569d55dc15e67d474c6f027f7b446f5420706b'
                )
            ) -- punks nonsense
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND contract_address IN (
                    '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
                )
            ) -- punks nonsense
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND topics [1] IS NULL
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
erc721s AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        PUBLIC.udf_hex_to_int(
            topics [3] :: STRING
        ) AS token_id,
        NULL AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND DATA = '0x'
        AND topics [3] IS NOT NULL
        AND contract_address NOT IN (
            '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
            '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        )
),
transfer_singles AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS operator_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
),
punks_bought AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) AS token_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
        NULL AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3'
),
punks_transfer AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        PUBLIC.udf_hex_to_int(
            DATA :: STRING
        ) AS token_id,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        NULL AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8'
),
regular_punk_transfers AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND contract_address IN (
            '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
            '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        )
),
legacy_tokens AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(segmented_data [0], 25, 40)) AS from_address,
        CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) AS to_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS token_id,
        NULL AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND topics [1] IS NULL
),
all_transfers AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        erc721s
    UNION ALL
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        transfer_singles
    UNION ALL
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        punks_bought
    UNION ALL
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        punks_transfer
    UNION ALL
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        legacy_tokens
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    project_name,
    from_address,
    to_address,
    all_transfers.token_id AS tokenId,
    token_metadata,
    erc1155_value,
    CASE
        WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
        ELSE 'other'
    END AS event_type,
    _log_id,
    _inserted_timestamp
FROM
    all_transfers t
    LEFT JOIN {{ ref('silver__nft_labels_temp') }}
    l
    ON t.contract_address = l.project_address
    AND t.token_id = l.token_id
WHERE
    to_address IS NOT NULL qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
