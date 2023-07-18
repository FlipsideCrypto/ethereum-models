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
        'EtherDeposited' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS TIME,
        TIME :: TIMESTAMP AS time_of_deposit,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x2c7d80ba9bc6395644b4ff4a878353ac20adeed6e23cead48c8cec7a58b6e719' --EtherDeposited
        AND contract_address = '0x54896f542f044709807f0d79033934d661d39fc1' --StafiEther

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
mints AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'TokensMinted' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS TIME,
        TIME :: TIMESTAMP AS time_of_deposit,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x6155cfd0fd028b0ca77e8495a60cbe563e8bce8611f0aad6fedbdaafc05d44a2' --TokensMinted
        AND contract_address = '0x9559aaa82d9649c7a7b220e7c461d2e74c9a3593' --StaFi (rETH)

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
    d.block_number,
    d.block_timestamp,
    d.origin_function_signature,
    d.origin_from_address,
    d.origin_to_address,
    d.tx_hash,
    d.event_index,
    d.event_name,
    d.contract_address,
    m.to_address AS sender,
    m.to_address AS recipient,
    d.amount AS eth_amount,
    d.amount_adj AS eth_amount_adj,
    m.amount AS token_amount,
    m.amount_adj AS token_amount_adj,
    '0x9559aaa82d9649c7a7b220e7c461d2e74c9a3593' AS token_address,
    'rETH' AS token_symbol,
    'stafi' AS platform,
    d.time_of_deposit,
    d._log_id,
    d._inserted_timestamp
FROM
    deposits d
    LEFT JOIN mints m
    ON d.tx_hash = m.tx_hash
WHERE
    m.to_address IS NOT NULL qualify (ROW_NUMBER() over (PARTITION BY d._log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
