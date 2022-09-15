{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['core']
) }}

WITH logdata AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address,
        event_name,
        topics,
        event_inputs,
        DATA,
        ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'

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
transfers AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address :: STRING AS contract_address,
        CASE
            WHEN event_name IN (
                'Transfer',
                'TransferSingle'
            ) THEN COALESCE(
                event_inputs :from :: STRING,
                event_inputs :_from :: STRING,
                event_inputs :fromAddress :: STRING
            )
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS from_address,
        CASE
            WHEN event_name IN (
                'Transfer',
                'TransferSingle'
            ) THEN COALESCE(
                event_inputs :to :: STRING,
                event_inputs :_to :: STRING,
                event_inputs :toAddress :: STRING
            )
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3' THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN event_name IN (
                'Transfer',
                'TransferSingle'
            ) THEN COALESCE(
                event_inputs :tokenId :: STRING,
                event_inputs :_id :: STRING,
                event_inputs :_tokenId :: STRING
            )
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN PUBLIC.udf_hex_to_int(
                DATA :: STRING
            )
            WHEN topics [0] :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3' THEN PUBLIC.udf_hex_to_int(
                topics [1] :: STRING
            )
        END AS nft_tokenid,
        event_inputs :_value :: STRING AS erc1155_value,
        ingested_at,
        _inserted_timestamp
    FROM
        logdata
    WHERE
        (
            event_name IN (
                'Transfer',
                'TransferSingle'
            )
            OR topics [0] :: STRING IN (
                '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3',
                '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8'
            )
        )
        AND nft_tokenid IS NOT NULL

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
--to address for punks
punk_bought AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        event_index,
        CASE
            WHEN event_name IN (
                'Transfer'
            ) THEN COALESCE(
                event_inputs :to :: STRING,
                event_inputs :_to :: STRING,
                event_inputs :toAddress :: STRING
            )
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS to_address
    FROM
        logdata
    WHERE
        (event_name IN ('Transfer')
        OR topics [0] :: STRING IN ('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'))
        AND contract_address IN (
            '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
            '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
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
-- next step handles the case where event names are not decoded
find_missing_events AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address :: STRING AS contract_address,
        topics,
        DATA,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN COALESCE(
                CONCAT('0x', SUBSTR(topics [1], 27, 40)),
                CONCAT('0x', SUBSTR(DATA, 27, 40))
            )
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN CONCAT('0x', SUBSTR(topics [2], 27, 40))
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN CONCAT('0x', SUBSTR(topics [1], 27, 40))
        END AS from_address,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN COALESCE(
                CONCAT('0x', SUBSTR(topics [2], 27, 40)),
                CONCAT('0x', SUBSTR(DATA, 91, 40))
            )
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN CONCAT('0x', SUBSTR(topics [3], 27, 40))
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN CONCAT('0x', SUBSTR(topics [2], 27, 40))
        END AS to_address,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN COALESCE(
                udf_hex_to_int(
                    topics [3] :: STRING
                ),
                udf_hex_to_int(SUBSTR(DATA, 160, 40))
            )
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN udf_hex_to_int(SUBSTR(DATA, 3, 64))
            WHEN topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' THEN udf_hex_to_int(SUBSTR(DATA, 3, 64))
        END AS tokenid,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN udf_hex_to_int(SUBSTR(DATA, 67, 64))
        END AS erc1155_value,
        ingested_at,
        _inserted_timestamp
    FROM
        logdata
    WHERE
        event_name IS NULL
        AND (
            topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND contract_address NOT IN (
                    '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
                ) -- excluding the punks contracts cuz they dont operate like every other token
            )
            OR topics [0] :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8'
        )
        AND contract_address IN (
            SELECT
                DISTINCT contract_address
            FROM
                transfers
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
all_transfers AS (
    SELECT
        A._log_id,
        A.block_number,
        A.tx_hash,
        A.block_timestamp,
        A.contract_address,
        A.from_address,
        CASE
            WHEN b.tx_hash IS NOT NULL THEN COALESCE(
                NULLIF(
                    A.to_address,
                    '0x0000000000000000000000000000000000000000'
                ),
                b.to_address
            )
            ELSE A.to_address
        END AS to_address,
        A.nft_tokenid AS tokenId,
        A.erc1155_value,
        A.ingested_at,
        A._inserted_timestamp,
        A.event_index
    FROM
        transfers A
        LEFT JOIN punk_bought b
        ON A.event_index -1 = b.event_index
        AND A.block_number = b.block_number
        AND A.tx_hash = b.tx_hash
    UNION ALL
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        tokenId,
        erc1155_value,
        ingested_at,
        _inserted_timestamp,
        event_index
    FROM
        find_missing_events
),
labels AS (
    SELECT
        address AS project_address,
        label,
        1 AS rnk
    FROM
        {{ ref('core__dim_labels') }}
    WHERE
        address IN (
            SELECT
                DISTINCT contract_address
            FROM
                all_transfers
        )
),
backup_meta AS (
    SELECT
        address AS project_address,
        NAME AS label,
        2 AS rnk
    FROM
        {{ source(
            'ethereum_silver',
            'token_meta_backup'
        ) }}
    WHERE
        address IN (
            SELECT
                DISTINCT contract_address
            FROM
                all_transfers
        )
),
meta_union AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        labels
    UNION ALL
    SELECT
        project_address,
        label,
        rnk
    FROM
        backup_meta
),
unique_meta AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        meta_union qualify(ROW_NUMBER() over(PARTITION BY project_address
    ORDER BY
        rnk ASC)) = 1
),
token_metadata AS (
    SELECT
        LOWER(contract_address) AS contract_address,
        token_id,
        token_metadata,
        project_name
    FROM
        {{ source(
            'flipside_gold_ethereum',
            'nft_metadata'
        ) }}
)
SELECT
    _log_id,
    block_number,
    tx_hash,
    block_timestamp,
    CASE
        WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
        ELSE 'other'
    END AS event_type,
    all_transfers.contract_address AS contract_address,
    COALESCE(
        label,
        project_name
    ) AS project_name,
    from_address,
    to_address,
    tokenId,
    erc1155_value,
    token_metadata,
    ingested_at,
    _inserted_timestamp,
    event_index
FROM
    all_transfers
    LEFT JOIN unique_meta
    ON unique_meta.project_address = all_transfers.contract_address
    LEFT JOIN token_metadata
    ON token_metadata.contract_address = all_transfers.contract_address
    AND all_transfers.tokenId = token_metadata.token_id qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
