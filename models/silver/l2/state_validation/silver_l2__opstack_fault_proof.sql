{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "state_block_number",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['curated']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    contract_address,
    '0x' || SUBSTR(
        topic_1,
        27
    ) :: STRING AS dispute_proxy_address,
    utils.udf_hex_to_int(topic_2) :: INT AS game_type,
    '0x' || (topic_3) :: STRING AS root_claim,
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    block_timestamp :: DATE >= '2024-06-01' --and tx_hash in ('0xa5553a2f1ab3192708983932a29a77e03c9cf90a6611d27e5d404427306655d1', '0xc1ce4e38720e7510d4cd2e4e5f64d83436b1a8e47d13be6be6f66cde8431da85')
    AND topic_0 = '0x5b565efe82411da98814f356d0e7bcb8f0219b8d970307c5afb4a6903a8b2e35' -- and to_address = '0x6b7da1647aa9684f54b2beeb699f91f31cd35fb9' -- optimism's anchor state registry
    AND origin_from_address = LOWER('0x473300df21D047806A082244b417f96b32f13A33') -- optimism state root proposer
