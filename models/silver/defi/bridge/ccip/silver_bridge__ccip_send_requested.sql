{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH on_ramp_set AS (

    SELECT
        block_timestamp,
        tx_hash,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :destChainSelector :: STRING
        ) AS destChainSelector,
        chain_name,
        decoded_log :onRamp :: STRING AS onRampAddress,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
        INNER JOIN {{ ref('silver_bridge__ccip_chain_seed') }}
        ON destChainSelector = chain_selector
    WHERE
        contract_address = LOWER('0x80226fc0Ee2b096224EeAc085Bb9a8cba1146f7D') -- ccip router
        AND topic_0 = '0x1f7d0ec248b80e5c0dde0ee531c4fc8fdb6ce9a2b3d90f560c74acd6a7202f23' -- onrampset
        AND tx_succeeded
        AND event_removed = FALSE

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
ccip_sent AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        contract_address,
        l.event_name,
        l.event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            '0x',
            segmented_data [13] :: STRING
        ) AS message_id,
        l.decoded_log,
        decoded_log :message :feeToken :: STRING AS fee_token,
        TRY_TO_NUMBER(
            decoded_log :message :feeTokenAmount :: STRING
        ) AS fee_token_amount,
        TRY_TO_NUMBER(
            decoded_log :message :gasLimit :: STRING
        ) AS gas_limit,
        TRY_TO_NUMBER(
            decoded_log :message :nonce :: STRING
        ) AS nonce,
        decoded_log :message :receiver :: STRING AS receiver,
        decoded_log :message :sender :: STRING AS sender,
        TRY_TO_NUMBER(
            decoded_log :message :sequenceNumber :: STRING
        ) AS sequence_number,
        TRY_TO_NUMBER(
            decoded_log :message :sourceChainSelector :: STRING
        ) AS source_chain_selector,
        destChainSelector AS dest_chain_selector,
        chain_name,
        decoded_log :message :tokenAmounts AS token_amounts,
        ARRAY_SIZE(
            decoded_log :message :tokenAmounts
        ) AS token_amounts_count,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
        l
        INNER JOIN on_ramp_set
        ON onRampAddress = contract_address
    WHERE
        topic_0 = '0xd0c3c799bf9e2639de44391e7f524d229b2b55f5b1ea94b2bf7da42f7243dddd' -- CCIPSendRequested
        AND tx_succeeded
        AND event_removed = FALSE

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    C.block_number,
    C.block_timestamp,
    C.origin_function_signature,
    C.origin_from_address,
    C.origin_to_address,
    C.tx_hash,
    C.event_name,
    C.event_index,
    'chainlink-ccip' AS platform,
    'v1' AS version,
    C.contract_address AS bridge_address,
    C.message_id,
    C.nonce,
    C.receiver,
    C.sender,
    C.receiver AS destination_chain_receiver,
    C.sequence_number,
    C.source_chain_selector,
    C.dest_chain_selector AS destination_chain_id,
    C.chain_name AS destination_chain,
    C.gas_limit,
    C.fee_token,
    -- Divide the fee evenly by the number of tokens in the array
    C.fee_token_amount / C.token_amounts_count AS fee_token_amount_per_token,
    C.token_amounts_count,
    TRY_TO_NUMBER(
        tokens.value :amount :: STRING
    ) AS amount_unadj,
    tokens.value :token :: STRING AS token_address,
    C._log_id,
    C.modified_timestamp
FROM
    ccip_sent C,
    LATERAL FLATTEN(
        input => C.token_amounts
    ) AS tokens
WHERE
    token_amounts_count > 0