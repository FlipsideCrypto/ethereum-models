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
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_index,
        topic_0,
        event_name,
        event_removed,
        decoded_log,
        TRY_TO_NUMBER(
            decoded_log :nonce :: STRING
        ) AS nonce,
        decoded_log :burnToken :: STRING AS burnToken,
        decoded_log :depositor :: STRING AS depositor,
        TRY_TO_NUMBER(
            decoded_log :amount :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :destinationDomain :: STRING
        ) AS destination_domain,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT(
                '0x',
                SUBSTR(
                    decoded_log :mintRecipient :: STRING,
                    25,
                    40
                )
            ) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(CONCAT('0x', decoded_log :mintRecipient :: STRING)) -- solana
            ELSE CONCAT(
                '0x',
                decoded_log :mintRecipient :: STRING
            ) -- other non-evm chains
        END AS mint_recipient,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT(
                '0x',
                SUBSTR(
                    decoded_log :destinationTokenMessenger :: STRING,
                    25,
                    40
                )
            ) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(
                CONCAT(
                    '0x',
                    decoded_log :destinationTokenMessenger :: STRING
                )
            ) -- solana
            ELSE CONCAT(
                '0x',
                decoded_log :destinationTokenMessenger :: STRING
            ) -- other non-evm chains
        END AS destination_token_messenger,
        CASE
            WHEN destination_domain IN (
                0,
                1,
                2,
                3,
                6,
                7,
                10
            ) THEN CONCAT(
                '0x',
                SUBSTR(
                    decoded_log :destinationCaller :: STRING,
                    25,
                    40
                )
            ) -- evm
            WHEN destination_domain = 5 THEN utils.udf_hex_to_base58(
                CONCAT(
                    '0x',
                    decoded_log :destinationCaller :: STRING
                )
            ) -- solana
            ELSE CONCAT(
                '0x',
                decoded_log :destinationCaller :: STRING
            ) -- other non-evm chains
        END AS destination_caller,
        modified_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
        AND topic_0 = '0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
    'circle-cctp-v2' AS platform,
    burnToken AS token_address,
    amount AS amount_unadj,
    depositor AS sender,
    origin_from_address AS receiver,
    mint_recipient AS destination_chain_receiver,
    destination_domain AS destination_chain_id,
    chain AS destination_chain,
    _log_id,
    modified_timestamp
FROM
    base_evt
    LEFT JOIN {{ ref('silver_bridge__cctp_chain_id_seed') }}
    d
    ON domain = destination_domain
