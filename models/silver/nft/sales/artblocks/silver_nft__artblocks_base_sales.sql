{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}
/*
Old artblocks contracts 
0x059edd72cd353df5106d2b9cc5ab83a52287ac3a
0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270

Current artblocks
0x99a9b7c1116f9ceeb1652de04d5969cce509b069

Artblocks collaborations
0x145789247973c5d612bf121e9e4eef84b63eb707
0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36
0x64780ce53f6e966e18a22af13a2f97369580ec11

Artblocks explorations
0x942bc2d3e7a589fe5bd4a5c6ef9727dfd82f5c8a
*/
WITH all_address AS (

    SELECT
        block_timestamp,
        created_contract_address AS address
    FROM
        {{ ref('silver__created_contracts') }}
    WHERE
        block_timestamp :: DATE >= '2020-01-01'
        AND creator_address IN (
            '0x6f8af4f14826405c5a361860de9f26acdaeab49b',
            -- old snowfro,
            '0xf3860788d1597cecf938424baabe976fac87dc26',
            -- snowfro,
            '0x96dc73c8b5969608c77375f085949744b5177660',
            -- deployer 1,
            '0xb8559af91377e5bab052a4e9a5088cb65a9a4d63' -- deployer 2
        )
),
function_sig_list AS (
    SELECT
        function_name,
        LEFT(
            function_signature,
            10
        ) AS function_sig,
        COUNT(1) AS txs
    FROM
        {{ ref('silver__flat_function_abis') }}
    WHERE
        (
            function_name LIKE 'purchase%'
            OR function_name LIKE 'purchaseTo%'
        )
        AND contract_address IN (
            SELECT
                address
            FROM
                all_address
        )
    GROUP BY
        ALL
),
raw_traces AS (
    SELECT
        *,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_input,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2020-11-01'
        AND to_address IN (
            SELECT
                address
            FROM
                all_address
        )
        AND trace_succeeded
        AND (
            function_sig IN (
                SELECT
                    function_sig
                FROM
                    function_sig_list
            )
            OR function_sig IN (
                '0x498dd0c1',
                -- currency addresss
                '0x8639415b' -- get primary revenue split
            )
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
purchase_function AS (
    -- to get mints via Minter / Settlement - in ETH
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        eth_value,
        eth_value_precise,
        function_sig,
        segmented_input,
        to_address AS purchase_contract,
        IFF (
            function_sig IN (
                SELECT
                    function_sig
                FROM
                    function_sig_list
                WHERE
                    function_name LIKE 'purchaseTo%'
            ),
            utils.udf_hex_to_int(
                segmented_input [1] :: STRING
            ),
            utils.udf_hex_to_int(
                segmented_input [0] :: STRING
            )
        ) :: INT AS project_id,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: STRING AS tokenid,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            project_id
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        function_sig IN (
            SELECT
                function_sig
            FROM
                function_sig_list
        )
),
collaboration_purchase AS (
    -- collaboration nfts in ETH only
    SELECT
        tx_hash,
        utils.udf_hex_to_int(
            segmented_input [0] :: STRING
        ) :: INT AS project_id,
        utils.udf_hex_to_int(
            segmented_input [1] :: STRING
        ) :: INT AS revenue_split_price_raw,
        utils.udf_hex_to_int(
            segmented_output [0] :: STRING
        ) :: INT AS revenue_split_platform_raw,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            project_id
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        raw_traces
    WHERE
        to_address IN (
            '0x145789247973c5d612bf121e9e4eef84b63eb707',
            '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
            '0x64780ce53f6e966e18a22af13a2f97369580ec11'
        )
        AND function_sig = '0x8639415b'
),
token_currency AS (
    SELECT
        tx_hash,
        '0x' || SUBSTR(
            segmented_output [0] :: STRING,
            25
        ) AS token_contract_address
    FROM
        raw_traces
    WHERE
        function_sig = '0x498dd0c1' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) = 1
),
pre_settlement_nft_mints AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address AS nft_address,
        (utils.udf_hex_to_int(topics [3] :: STRING)) :: INT AS project_id,
        (utils.udf_hex_to_int(topics [2] :: STRING)) :: STRING AS tokenid,
        '0x' || SUBSTR(
            topics [2] :: STRING,
            25
        ) AS nft_to_address_logs,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            nft_address
            ORDER BY
                event_index ASC
        ) AS row_num,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2020-11-01'
        AND contract_address IN (
            SELECT
                address
            FROM
                all_address
            WHERE
                block_timestamp :: DATE < '2022-10-11'
        )
        AND topics [0] :: STRING IN (
            '0x94c792774c59479f7bd68442f3af3691c02123a5aabee8b6f9116d8af8aa6669',
            '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
post_settlement_nft_mints AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address AS nft_address,
        tokenid,
        to_address AS nft_to_address_logs,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-10-11'
        AND contract_address IN (
            SELECT
                address
            FROM
                all_address
            WHERE
                block_timestamp :: DATE >= '2022-10-11'
        )
        AND from_address = '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
all_nft_mints AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        nft_address,
        project_id,
        tokenid,
        nft_to_address_logs,
        _log_id,
        _inserted_timestamp
    FROM
        pre_settlement_nft_mints
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        nft_address,
        NULL AS project_id,
        tokenid,
        nft_to_address_logs,
        _log_id,
        _inserted_timestamp
    FROM
        post_settlement_nft_mints
),
final_mint_base AS (
    SELECT
        m.block_number,
        m.block_timestamp,
        tx_position,
        m.tx_hash,
        IFF(
            p.tx_hash IS NULL,
            'external_mint',
            'regular'
        ) AS mint_type,
        event_index,
        trace_index,
        from_address,
        to_address,
        COALESCE(
            eth_value,
            0
        ) AS mint_eth_value,
        function_sig,
        segmented_input,
        purchase_contract,
        nft_address,
        COALESCE(
            m.project_id,
            p.project_id
        ) AS project_id,
        m.tokenid,
        nft_to_address_logs,
        intra_tx_grouping,
        _log_id,
        _inserted_timestamp
    FROM
        all_nft_mints m
        LEFT JOIN purchase_function p USING (
            tx_hash,
            tokenid
        )
    WHERE
        nft_address IN (
            '0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270',
            '0x99a9b7c1116f9ceeb1652de04d5969cce509b069',
            '0x059edd72cd353df5106d2b9cc5ab83a52287ac3a',
            '0x145789247973c5d612bf121e9e4eef84b63eb707',
            '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
            '0x64780ce53f6e966e18a22af13a2f97369580ec11',
            '0x942bc2d3e7a589fe5bd4a5c6ef9727dfd82f5c8a'
        )
),
token_transfers AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        event_index,
        contract_address,
        raw_amount
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_timestamp :: DATE >= '2020-11-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    tx_position,
    b.tx_hash,
    intra_tx_grouping,
    mint_type,
    b.event_index,
    trace_index,
    b.from_address,
    b.to_address,
    mint_eth_value,
    revenue_split_price_raw,
    revenue_split_platform_raw,
    function_sig,
    segmented_input,
    purchase_contract,
    nft_address,
    project_id,
    tokenid,
    nft_to_address_logs,
    t.tx_hash AS transfers_hash,
    t.from_address AS transfers_from_address,
    t.to_address AS transfers_to_address,
    t.event_index AS transfers_event_index,
    t.contract_address AS transfers_contract_address,
    t.raw_amount AS transfers_raw_amount,
    C.token_contract_address,
    _log_id,
    _inserted_timestamp
FROM
    final_mint_base b
    LEFT JOIN collaboration_purchase USING (
        tx_hash,
        project_id,
        intra_tx_grouping
    )
    LEFT JOIN token_transfers t
    ON b.tx_hash = t.tx_hash
    AND b.from_address = t.from_address
    LEFT JOIN token_currency C
    ON C.tx_hash = b.tx_hash
    AND C.token_contract_address = t.contract_address
