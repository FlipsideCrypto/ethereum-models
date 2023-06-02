{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
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
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS user,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS referall,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS ethAmount,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS wBETHAmount,
        (ethAmount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        (wBETHAmount / pow(10, 18)) :: FLOAT AS wbeth_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xe32c4b34261b430739ef30d727d062f9fdd6410be2080e6fd875a6015f40de83' --DepositEth
        AND contract_address = '0xa2e3356610840701bdf5611a53974510ae27e2e1' --Wrapped Binance Beacon ETH (wBETH)
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
    contract_address,
    user AS sender,
    user AS recipient, 
    referall AS referrer_address,
    ethAmount AS eth_amount,
    eth_amount_adj,
    wBETHAmount AS token_amount,
    wbeth_amount_adj AS token_amount_adj,
    _log_id,
    _inserted_timestamp
FROM
    deposits