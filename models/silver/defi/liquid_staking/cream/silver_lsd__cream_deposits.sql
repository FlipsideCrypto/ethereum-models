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
        'DepositEvent' AS event_name,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS account,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS creth2_amount,
        (creth2_amount / pow(10, 18)) :: FLOAT AS creth2_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xad40ae5dc69974ba932d08b0a608e89109412d41d04850f5196f144875ae2660' --DepositEvent
        AND contract_address = '0xcbc1065255cbc3ab41a6868c22d1f1c573ab89fd' --Cream ETH 2 (CRETH2)
        AND tx_status = 'SUCCESS'

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
    account AS sender,
    account AS recipient,
    eth_amount,
    eth_amount_adj,
    creth2_amount AS token_amount,
    creth2_amount_adj AS token_amount_adj,
    contract_address AS token_address,
    'CRETH2' AS token_symbol,
    'cream-finance' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    deposits