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
        'Deposit' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS assets,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS shares,
        (assets / pow(10, 18)) :: FLOAT AS assets_adj,
        (shares / pow(10, 18)) :: FLOAT AS shares_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7' --Deposit
        AND contract_address = '0xac3e018457b222d93114458476f3e3416abbe38f' --Staked Frax Ether (sfrxETH)
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
    caller AS sender,
    owner AS recipient, 
    assets AS eth_amount,
    assets_adj AS eth_amount_adj,
    shares AS token_amount,
    shares_adj AS token_amount_adj,
    contract_address AS token_address,
    'sfrxETH' AS token_symbol,
    'frax-ether' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    deposits