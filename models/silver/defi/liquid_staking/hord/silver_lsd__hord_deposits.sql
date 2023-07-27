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
        'Deposit' AS event_name,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS account,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS eth_amount,
        (eth_amount / pow(10, 18)) :: FLOAT AS eth_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' --Deposit
        AND contract_address = '0x5bbe36152d3cd3eb7183a82470b39b29eedf068b' --Hord (hETH)

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
        'HETHMinted' AS event_name,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS account,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x7fc2fd3fd75a3920468e7ebb35c1c6c24d63052113a15ce8a4d461bf9b8c7f84' --HETHMinted
        AND contract_address = '0x5bbe36152d3cd3eb7183a82470b39b29eedf068b' --HordETH (hETH)

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
    d.account AS sender,
    m.account AS recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    d.contract_address AS token_address,
    'hETH' AS token_symbol,
    'hord' AS platform,
    d._log_id,
    d._inserted_timestamp
FROM
    deposits d
    LEFT JOIN mints m
    ON d.tx_hash = m.tx_hash
    AND d.account = m.account
