{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'allbridge' AS platform,
        event_index,
        'TokensSent' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS destinationChainId,
        origin_from_address AS sender,
        origin_from_address AS recipient,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)) AS nonce,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS messenger,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0x9cd6008e8d4ebd34fd9d022278fec7f95d133780ecc1a0dea459fae3e9675390' --TokensSent
        AND contract_address = '0x609c690e8f7d68a59885c9132e812eebdaaf0c9e' --Allbridge
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
lp_evt AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS sender,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)) AS vUsdAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)) AS fee,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0xa930da1d3f27a25892307dd59cec52dd9b881661a0f20364757f83a0da2f6873' --SwappedToVUsd
        AND contract_address IN (
            '0x7dbf07ad92ed4e26d5511b4f285508ebf174135d',
            --USDT LP
            '0xa7062bba94c91d565ae33b893ab5dfaf1fc57c4d' --USDC LP
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base_evt
        )
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_to_address,
    s.contract_address AS bridge_address,
    s.event_index,
    s.event_name,
    s.platform,
    lp.amount,
    lp.token AS token_address,
    s.sender,
    s.recipient AS receiver,
    C.chain AS destination_chain,
    s.destinationChainId AS destination_chain_id,
    CASE
        WHEN C.chain = 'solana' THEN utils.udf_hex_to_base58(CONCAT('0x', s.segmented_data [1] :: STRING))
        WHEN C.chain = 'stellar' THEN s.segmented_data [1] :: STRING
        ELSE CONCAT(
            '0x',
            SUBSTR(
                s.segmented_data [1] :: STRING,
                25,
                40
            )
        )
    END AS destination_chain_receiver,
    CASE
        WHEN C.chain = 'solana' THEN utils.udf_hex_to_base58(CONCAT('0x', s.segmented_data [3] :: STRING))
        WHEN C.chain = 'stellar' THEN s.segmented_data [3] :: STRING
        ELSE CONCAT(
            '0x',
            SUBSTR(
                s.segmented_data [3] :: STRING,
                25,
                40
            )
        )
    END AS destination_chain_token,
    s.tx_succeeded,
    s._log_id,
    s._inserted_timestamp
FROM
    base_evt s
    INNER JOIN lp_evt lp
    ON s.tx_hash = lp.tx_hash
    AND s.block_number = lp.block_number
    LEFT JOIN {{ ref('silver_bridge__allbridge_chain_id_seed') }} C
    ON s.destinationChainId = C.chain_id qualify(ROW_NUMBER() over (PARTITION BY s._log_id
ORDER BY
    s._inserted_timestamp DESC)) = 1
