{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','liquid_staking','curated']
) }}

WITH deposits AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Deposited' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS depositor,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS beneficiar,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS tokens,
        (tokens / pow(10, 18)) :: FLOAT AS tokens_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS releaseTime,
        (releaseTime / pow(10, 18)) :: FLOAT AS release_time_adj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xf5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5' --Deposited
        AND contract_address = '0xcf5ea1b38380f6af39068375516daf40ed70d299' --TransparentUpgradeableProxy
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
    event_name,
    contract_address,
    depositor AS sender,
    beneficiar AS recipient,
    tokens AS eth_amount,
    tokens_adj AS eth_amount_adj,
    releaseTime AS token_amount,
    release_time_adj AS token_amount_adj,
    '0xa35b1b31ce002fbf2058d22f30f95d405200a15b' AS token_address,
    'ETHx' AS token_symbol,
    'stader' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    deposits