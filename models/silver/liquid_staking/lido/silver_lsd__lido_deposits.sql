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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS referral,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x96a25c8ce0baabc1fdefd93e9ed25d8e092a3332f3aa9a41722b5697231d1d1a' --Submitted
        AND contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' --Lido stETH Token (stETH)
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
transfers AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS value,
        (value / pow(10, 18)) :: FLOAT AS value_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        AND contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' --Lido stETH Token (stETH)
        AND from_address = '0x0000000000000000000000000000000000000000'
        AND to_address IN (
            SELECT
                sender
            FROM
                deposits
        )
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
    d.contract_address,
    d.sender,
    t.to_address AS recipient,
    d.referral AS referral_address,
    d.amount AS eth_amount,
    d.amount_adj AS eth_amount_adj,
    t.value AS token_amount,
    t.value_adj AS token_amount_adj,
    d._log_id,
    d._inserted_timestamp
FROM
    deposits d
    LEFT JOIN transfers t
    ON d.tx_hash = t.tx_hash qualify (ROW_NUMBER() over (PARTITION BY d._log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1