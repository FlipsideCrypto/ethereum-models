{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','liquid_staking','curated']
) }}

WITH deposit_logs AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Mint' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS minter,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8' --Mint/Deposit
        AND contract_address = '0xa2e3356610840701bdf5611a53974510ae27e2e1' --Wrapped Binance Beacon ETH (wBETH)
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
deposit_traces AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        VALUE * pow(
            10,
            18
        ) AS eth_amount,
        VALUE AS eth_amount_adj,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2023-04-19'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                deposit_logs
        )
        AND from_address IN (
            SELECT
                to_address
            FROM
                deposit_logs
        )
        AND tx_succeeded
        AND trace_succeeded
)
SELECT
    l.block_number,
    l.block_timestamp,
    l.origin_function_signature,
    l.origin_from_address,
    l.origin_to_address,
    l.tx_hash,
    l.event_index,
    l.event_name,
    l.contract_address,
    minter AS sender,
    l.to_address AS recipient,
    CASE
        WHEN eth_amount = 0 THEN amount
        ELSE eth_amount
    END AS eth_amount,
    CASE
        WHEN eth_amount_adj = 0 THEN amount_adj
        ELSE eth_amount_adj
    END AS eth_amount_adj,
    amount AS token_amount,
    amount_adj AS token_amount_adj,
    l.contract_address AS token_address,
    'wBETH' AS token_symbol,
    'binance' AS platform,
    _log_id,
    l._inserted_timestamp
FROM
    deposit_logs l
    LEFT JOIN deposit_traces t
    ON l.tx_hash = t.tx_hash
    AND t.from_address = l.to_address qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    l._inserted_timestamp)) = 1
