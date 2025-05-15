{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
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
        'axie_infinity' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"receipt" :"info" :"quantity" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"receipt" :"ronin" :"chainId" :: STRING
        ) AS chainId,
        decoded_log :"receipt" :"mainchain" :"addr" :: STRING AS sender,
        decoded_log :"receipt" :"ronin" :"addr" :: STRING AS receiver,
        decoded_log :"receipt" :"mainchain" :"tokenAddr" :: STRING AS token_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xd7b25068d9dc8d00765254cfb7f5070f98d263c8d68931d937c7362fa738048b' -- DepositRequested
        AND contract_address = '0x64192819ac13ef72bf6b5ae239ac672b43a9af08' -- Axie Infinity: Ronin Bridge V2
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
    contract_address AS bridge_address,
    NAME AS platform,
    sender,
    receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainId AS destination_chain_id,
    token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt