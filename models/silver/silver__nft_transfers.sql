{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE', 'contract_address'],
    tags = ['core']
) }}

WITH base AS (

    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        event_index :: FLOAT AS event_index,
        contract_address,
        topics,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND (
            (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND DATA = '0x'
                AND topics [3] IS NOT NULL
            ) --erc721s TransferSingle event
            OR (
                topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) --erc1155s
            OR (
                topics [0] :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
            ) --erc1155s TransferBatch event
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS erc1155_value,
        _inserted_timestamp,
        event_index
    FROM
        base
    WHERE
        topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
),
transfer_batch_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS operator_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
        contract_address,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: STRING AS tokenid_length,
        tokenid_length AS quantity_length,
        _log_id,
        _inserted_timestamp
    FROM
        base
    WHERE
        topics [0] :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
),
flattened AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        event_index,
        operator_address,
        from_address,
        to_address,
        contract_address,
        INDEX,
        VALUE,
        tokenid_length,
        quantity_length,
        '2' + tokenid_length AS tokenid_indextag,
        '4' + tokenid_length AS quantity_indextag_start,
        '4' + tokenid_length + tokenid_length AS quantity_indextag_end,
        CASE
            WHEN INDEX BETWEEN 3
            AND (
                tokenid_indextag
            ) THEN 'tokenid'
            WHEN INDEX BETWEEN (
                quantity_indextag_start
            )
            AND (
                quantity_indextag_end
            ) THEN 'quantity'
            ELSE NULL
        END AS label
    FROM
        transfer_batch_raw,
        LATERAL FLATTEN (
            input => segmented_data
        )
),
tokenid_list AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        tx_hash,
        event_index,
        operator_address,
        from_address,
        to_address,
        contract_address,
        utils.udf_hex_to_int(
            VALUE :: STRING
        ) AS tokenId,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                INDEX ASC
        ) AS tokenid_order
    FROM
        flattened
    WHERE
        label = 'tokenid'
),
quantity_list AS (
    SELECT
        tx_hash,
        event_index,
        utils.udf_hex_to_int(
            VALUE :: STRING
        ) AS quantity,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                INDEX ASC
        ) AS quantity_order
    FROM
        flattened
    WHERE
        label = 'quantity'
),
transfer_batch_final AS (
    SELECT
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp,
        t.tx_hash,
        t.event_index,
        operator_address,
        from_address,
        to_address,
        contract_address,
        t.tokenId AS token_id,
        q.quantity AS erc1155_value,
        tokenid_order AS intra_event_index
    FROM
        tokenid_list t
        INNER JOIN quantity_list q
        ON t.tx_hash = q.tx_hash
        AND t.event_index = q.event_index
        AND t.tokenid_order = q.quantity_order
),
punks_bought AS (
    SELECT
        _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id
        ) AS _log_id
    FROM
        erc721s
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id
        ) AS _log_id
    FROM
        transfer_singles
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id,
            '-',
            intra_event_index
        ) AS _log_id
    FROM
        transfer_batch_final
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id
        ) AS _log_id
    FROM
        punks_bought
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id
        ) AS _log_id
    FROM
        punks_transfer
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        contract_address,
        from_address,
        to_address,
        token_id,
        erc1155_value,
        _inserted_timestamp,
        event_index,
        CONCAT(
            _log_id,
            '-',
            contract_address,
            '-',
            token_id
        ) AS _log_id
    FROM
        legacy_tokens
),
final_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        from_address,
        to_address,
        all_transfers.token_id AS tokenId,
        erc1155_value,
        CASE
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
            ELSE 'other'
        END AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        all_transfers
    WHERE
        to_address IS NOT NULL
),
labels_only AS (
    SELECT
        DISTINCT project_address AS project_address,
        project_name
    FROM
        {{ ref('silver__nft_labels_temp') }}
    WHERE
        project_address IS NOT NULL
),
metadata AS (
    SELECT
        project_address,
        token_id,
        token_metadata
    FROM
        {{ ref('silver__nft_labels_temp') }}
    WHERE
        project_address IS NOT NULL
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    l.project_name,
    from_address,
    to_address,
    tokenId,
    m.token_metadata,
    erc1155_value,
    event_type,
    _log_id,
    _inserted_timestamp
FROM
    final_base
    LEFT JOIN labels_only l
    ON final_base.contract_address = l.project_address
    LEFT JOIN metadata m
    ON final_base.contract_address = m.project_address
    AND final_base.tokenId = m.token_id qualify ROW_NUMBER() over (
        PARTITION BY _log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
