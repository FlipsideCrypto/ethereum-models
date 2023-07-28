{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH requests AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'WithdrawalRequested' AS event_name,
        contract_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [1] :: STRING
            )
        ) AS requestId,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS requestor,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS owner,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amountOfStETH,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amountOfShares,
        (amountOfStETH / pow(10, 18)) :: FLOAT AS amount_of_steth_adj,
        (amountOfShares / pow(10, 18)) :: FLOAT AS amount_of_shares_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xf0cb471f23fb74ea44b8252eb1881a2dca546288d9f6e90d1a0e82fe0ed342ab' --WithdrawalRequested
        AND contract_address = '0x889edc2edab5f40e902b864ad4d7ade8e412f9b1' --Lido: stETH Withdrawal NFT (unstETH)
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
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
    requestId AS request_id,
    requestor,
    owner,
    amountOfStETH AS amount_of_steth,
    amountOfShares AS amount_of_shares,
    amount_of_steth_adj,
    amount_of_shares_adj,
    '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' AS token_address,
    'stETH' AS token_symbol,
    'lido' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    requests