{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_real_time']
) }}

WITH withdrawals AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'EthUnstake' AS event_name,
        contract_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [1] :: STRING
            )
        ) AS operator_id,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS target_operator_id,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS sender,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc2d18d1ab67a48ae80c3ef1d20c2f2a97201a23db7ca49e5de1edf05610fb003' --EthUnstake
        AND contract_address = '0x8103151e2377e78c04a3d2564e20542680ed3096' --ERC1967Proxy

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
    sender,
    sender AS recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    '0xc6572019548dfeba782ba5a2093c836626c7789a' AS token_address,
    'nETH' AS token_symbol,
    'nodedao' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    withdrawals