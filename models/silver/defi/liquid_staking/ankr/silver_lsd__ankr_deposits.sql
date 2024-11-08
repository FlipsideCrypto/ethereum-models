{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        'StakeConfirmed' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS staker,
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
        topics [0] :: STRING = '0x995d6cdbf356b73aa4dff24e951558cc155c9bb0397786ec4a142f9470f50007' --StakeConfirmed
        AND contract_address = '0x84db6ee82b7cf3b47e8f19270abde5718b936670' --ankr ETH2 Staking
        AND tx_succeeded
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
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    staker AS sender,
    staker AS recipient,
    amount AS eth_amount,
    amount_adj AS eth_amount_adj,
    eth_amount AS token_amount,
    eth_amount_adj AS token_amount_adj,
    '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb' AS token_address,
    'ankrETH' AS token_symbol,
    'ankr' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    deposits