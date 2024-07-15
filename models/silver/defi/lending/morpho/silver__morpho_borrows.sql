{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH borrow_traces AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        CONCAT('0x', SUBSTR(segmented_input [0] :: STRING, 25)) AS loan_token,
        CONCAT('0x', SUBSTR(segmented_input [1] :: STRING, 25)) AS collateral_token,
        CONCAT('0x', SUBSTR(segmented_input [2] :: STRING, 25)) AS oracle_address,
        CONCAT('0x', SUBSTR(segmented_input [3] :: STRING, 25)) AS irm_address,
        TO_NUMBER(
            REGEXP_SUBSTR(
                _call_id,
                'CALL_([0-9]+)',
                1,
                1,
                'e',
                1
            )
        ) AS call_number,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                TO_NUMBER(
                    REGEXP_SUBSTR(
                        _call_id,
                        'CALL_([0-9]+)',
                        1,
                        1,
                        'e',
                        1
                    )
                )
        ) AS call_rank,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [4] :: STRING
            )
        ) AS lltv,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [5] :: STRING
            )
        ) AS borrow_amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [6] :: STRING
            )
        ) AS shares,
        CONCAT('0x', SUBSTR(segmented_input [7] :: STRING, 25)) AS on_behalf_address,
        CONCAT('0x', SUBSTR(segmented_input [8] :: STRING, 25)) AS receiver_address,
        t._call_id,
        t._inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
        t
    WHERE
        to_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' --Morpho Blue
        AND function_sig = '0x50d8cd4b'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_trace_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
traces AS(
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        function_sig,
        call_rank,
        segmented_input,
        loan_token,
        collateral_token,
        t.oracle_address,
        t.irm_address,
        t.lltv,
        m.market_id,
        borrow_amount,
        on_behalf_address,
        receiver_address,
        t._call_id,
        t._inserted_timestamp
    FROM
        borrow_traces t
        LEFT JOIN {{ ref('silver__morpho_markets') }}
        m
        ON t.oracle_address = m.oracle_address
        AND t.lltv = m.lltv
        AND t.loan_token = m.loan_address
        AND t.collateral_token = m.collateral_address
),
logs_level AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS event_rank,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics [1] :: STRING AS market_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS reciever,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS caller,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS borrow_amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS shares,
        l._inserted_timestamp,
        l._log_id,
        'Morpho Blue' AS morpho_version,
        origin_from_address AS borrower_address,
        contract_address AS lending_pool_contract
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] :: STRING = '0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43'
        AND tx_status = 'SUCCESS'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                traces
        )
)
,
joined_data AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        b.event_index,
        b.origin_from_address,
        b.origin_to_address,
        b.origin_function_signature,
        b.contract_address,
        l.loan_token AS market,
        l.market_id,
        l.borrow_amount AS amount_unadj,
        l.borrow_amount / pow(10, C.decimals) AS amount,
        C.symbol,
        C.decimals,
        b.borrower_address,
        b.lending_pool_contract,
        b.morpho_version AS platform,
        'ethereum' AS blockchain,
        b._inserted_timestamp,
        b._log_id,
        l._call_id,
        L.CALL_RANK,
        B.EVENT_RANK,
        l._inserted_timestamp AS _inserted_trace_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY l.tx_hash, l.market_id, l.borrow_amount, l.receiver_address, l.on_behalf_address
            ORDER BY l.call_rank, b.event_rank
        ) AS row_num
FROM
    traces l
    LEFT JOIN logs_level b
    ON l.tx_hash = b.tx_hash
    AND l.market_id = b.market_id
    AND l.borrow_amount = b.borrow_amount
    AND l.receiver_address = b.reciever
    AND l.on_behalf_address = b.onBehalfOf
    --AND l.call_rank = b.event_rank
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON address = l.loan_token 
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    market,
    market_id,
    amount_unadj,
    amount,
    symbol,
    decimals,
    borrower_address,
    lending_pool_contract,
    platform,
    blockchain,
    ROW_NUM,
    EVENT_RANK,
    CALL_RANK,
    _inserted_timestamp,
    _log_id,
    _call_id,
    _inserted_trace_timestamp
FROM
    joined_data 
WHERE 
    ROW_NUM = 1
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    market,
    market_id,
    amount_unadj,
    amount,
    symbol,
    decimals,
    borrower_address,
    lending_pool_contract,
    platform,
    blockchain,
    ROW_NUM,
    EVENT_RANK,
    CALL_RANK,
    _inserted_timestamp,
    _log_id,
    _call_id,
    _inserted_trace_timestamp
FROM
    joined_data 
WHERE 
    EVENT_RANK = CALL_RANK
AND ROW_NUM <> 1