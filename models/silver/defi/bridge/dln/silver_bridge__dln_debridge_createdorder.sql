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
        'dln_debridge' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [24] :: STRING, 1, 40)) AS token_address,
        decoded_log :"affiliateFee" :: STRING AS affiliateFee,
        decoded_log :"metadata" :: STRING AS metadata,
        TRY_TO_NUMBER(
            decoded_log :"nativeFixFee" :: STRING
        ) AS nativeFixFee,
        decoded_log :"order" AS order_obj,
        decoded_log :"order" :"allowedCancelBeneficiarySrc" :: STRING AS allowedCancelBeneficiarySrc,
        decoded_log :"order" :"allowedTakerDst" :: STRING AS allowedTakerDst,
        decoded_log :"order" :"externalCall" :: STRING AS externalCall,
        TRY_TO_NUMBER(
            decoded_log :"order" :"giveAmount" :: STRING
        ) AS giveAmount,
        TRY_TO_NUMBER(
            decoded_log :"order" :"giveChainId" :: STRING
        ) AS giveChainId,
        decoded_log :"order" :"givePatchAuthoritySrc" :: STRING AS givePatchAuthoritySrc,
        decoded_log :"order" :"giveTokenAddress" :: STRING AS giveTokenAddress,
        TRY_TO_NUMBER(
            decoded_log :"order" :"makerOrderNonce" :: STRING
        ) AS makerOrderNonce,
        decoded_log :"order" :"makerSrc" :: STRING AS makerSrc,
        decoded_log :"order" :"orderAuthorityAddressDst" :: STRING AS orderAuthorityAddressDst,
        CONCAT('0x', LEFT(segmented_data [28] :: STRING, 40)) AS receiverDst,
        TRY_TO_NUMBER(
            decoded_log :"order" :"takeAmount" :: STRING
        ) AS takeAmount,
        TRY_TO_NUMBER(
            decoded_log :"order" :"takeChainId" :: STRING
        ) AS takeChainId,
        decoded_log :"order" :"takeTokenAddress" :: STRING AS takeTokenAddress,
        decoded_log :"orderId" :: STRING AS orderId,
        TRY_TO_NUMBER(
            decoded_log :"percentFee" :: STRING
        ) AS percentFee,
        TRY_TO_NUMBER(
            decoded_log :"referralCode" :: STRING
        ) AS referralCode,
        decoded_log AS decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xfc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471' --CreatedOrder
        AND contract_address = '0xef4fb24ad0916217251f553c0596f8edc630eb66' --Dln: Source
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
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_succeeded,
    contract_address AS bridge_address,
    NAME AS platform,
    origin_from_address AS sender,
    receiverDst AS receiver,
    CASE
        WHEN takeChainId :: STRING = '7565164' THEN utils.udf_hex_to_base58(CONCAT('0x', segmented_data [28] :: STRING))
        ELSE receiverDst
    END AS destination_chain_receiver,
    giveAmount AS amount,
    takeChainId AS destination_chain_id,
    CASE
        WHEN destination_chain_id :: STRING = '7565164' THEN 'solana'
        ELSE NULL
    END AS destination_chain,
    CASE
        WHEN token_address = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE token_address
    END AS token_address,
    decoded_flat,
    order_obj,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
